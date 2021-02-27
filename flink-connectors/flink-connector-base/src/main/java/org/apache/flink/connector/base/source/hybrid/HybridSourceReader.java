/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Hybrid source reader that delegates to the actual source reader.
 *
 * <p>This reader is setup with a sequence of underlying source readers. At a given point in time,
 * one of these readers is active. Underlying readers are opened and closed on demand as determined
 * by the enumerator, which selects the active reader via {@link SwitchSourceEvent}.
 *
 * <p>When the underlying reader has consumed all input, {@link HybridSourceReader} sends {@link
 * SourceReaderFinishedEvent} to the coordinator and waits for the {@link SwitchSourceEvent}.
 */
public class HybridSourceReader<T, SplitT extends SourceSplit>
        implements SourceReader<T, HybridSourceSplit<SplitT>> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceReader.class);
    // Controls the interval at which to indicate to the coordinator that the current reader
    // has consumed all input and the next source can be activated. This is necessary to not flood
    // the coordinator with duplicate events.
    private static final int SOURCE_READER_FINISHED_EVENT_DELAY = 250;
    private SourceReaderContext readerContext;
    private List<SourceReader<?, SplitT>> realReaders;
    private int currentSourceIndex = -1;
    private long lastCheckpointId = -1;
    private SourceReader<?, SplitT> currentReader;
    private long lastReaderFinishedMs;

    public HybridSourceReader(
            SourceReaderContext readerContext, List<SourceReader<?, SplitT>> readers) {
        this.readerContext = readerContext;
        this.realReaders = readers;
    }

    @Override
    public void start() {
        setCurrentReader(0);
    }

    @Override
    public InputStatus pollNext(ReaderOutput output) throws Exception {
        InputStatus status = currentReader.pollNext(output);
        if (status == InputStatus.END_OF_INPUT) {
            // trap END_OF_INPUT if this wasn't the final reader
            LOG.debug(
                    "End of input subtask={} sourceIndex={} {}",
                    readerContext.getIndexOfSubtask(),
                    currentSourceIndex,
                    currentReader);
            if (currentSourceIndex + 1 < realReaders.size()) {
                // signal coordinator to advance readers
                long currentMillis = System.currentTimeMillis();
                if (lastReaderFinishedMs + SOURCE_READER_FINISHED_EVENT_DELAY < currentMillis) {
                    lastReaderFinishedMs = currentMillis;
                    readerContext.sendSourceEventToCoordinator(
                            new SourceReaderFinishedEvent(currentSourceIndex, lastCheckpointId));
                }
                // more data will be available from the next reader
                return InputStatus.MORE_AVAILABLE;
            }
        }
        return status;
    }

    @Override
    public List<HybridSourceSplit<SplitT>> snapshotState(long checkpointId) {
        this.lastCheckpointId = checkpointId;
        List<SplitT> state = currentReader.snapshotState(checkpointId);
        return wrappedSplits(currentSourceIndex, state);
    }

    public static <SplitT extends SourceSplit> List<HybridSourceSplit<SplitT>> wrappedSplits(
            int readerIndex, List<SplitT> state) {
        List<HybridSourceSplit<SplitT>> wrappedSplits = new ArrayList<>(state.size());
        for (SplitT split : state) {
            wrappedSplits.add(new HybridSourceSplit<>(readerIndex, split));
        }
        return wrappedSplits;
    }

    public static <SplitT extends SourceSplit> List<SplitT> unwrappedSplits(
            List<HybridSourceSplit<SplitT>> splits) {
        List<SplitT> unwrappedSplits = new ArrayList<>(splits.size());
        for (HybridSourceSplit<SplitT> split : splits) {
            unwrappedSplits.add(split.getWrappedSplit());
        }
        return unwrappedSplits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return currentReader.isAvailable();
    }

    @Override
    public void addSplits(List<HybridSourceSplit<SplitT>> splits) {
        LOG.info(
                "### Adding splits subtask={} sourceIndex={} {} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader,
                splits);
        List<SplitT> realSplits = new ArrayList<>(splits.size());
        for (HybridSourceSplit split : splits) {
            Preconditions.checkState(
                    split.sourceIndex() == currentSourceIndex,
                    "Split %s while current source is %s",
                    split,
                    currentSourceIndex);
            realSplits.add((SplitT) split.getWrappedSplit());
        }
        currentReader.addSplits(realSplits);
    }

    @Override
    public void notifyNoMoreSplits() {
        currentReader.notifyNoMoreSplits();
        LOG.debug(
                "No more splits for reader subtask={} sourceIndex={} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof SwitchSourceEvent) {
            SwitchSourceEvent sse = (SwitchSourceEvent) sourceEvent;
            LOG.debug(
                    "Switch source event: subtask={} sourceIndex={}",
                    readerContext.getIndexOfSubtask(),
                    sse.sourceIndex());
            setCurrentReader(sse.sourceIndex());
        } else {
            currentReader.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    public void close() throws Exception {
        currentReader.close();
        LOG.debug(
                "Reader closed: subtask={} sourceIndex={} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader);
    }

    private void setCurrentReader(int index) {
        Preconditions.checkState(
                currentSourceIndex <= index, "reader index monotonically increasing");
        Preconditions.checkState(index < realReaders.size(), "invalid reader index: %s", index);
        if (currentSourceIndex == index) {
            LOG.debug(
                    "Reader already set to process source: subtask={} sourceIndex={} {}",
                    readerContext.getIndexOfSubtask(),
                    currentSourceIndex);
            return;
        }
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close current reader", e);
            }
            LOG.debug(
                    "Reader closed: subtask={} sourceIndex={} {}",
                    readerContext.getIndexOfSubtask(),
                    currentSourceIndex,
                    currentReader);
        }
        SourceReader<?, SplitT> reader = realReaders.get(index);
        reader.start();
        currentSourceIndex = index;
        currentReader = reader;
        LOG.debug(
                "Reader started: subtask={} sourceIndex={} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                reader);
    }
}
