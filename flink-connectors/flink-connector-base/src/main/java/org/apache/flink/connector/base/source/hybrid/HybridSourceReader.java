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

import java.util.ArrayDeque;
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
 * SourceReaderFinishedEvent} to the coordinator.
 *
 * <p>The reader does not make assumptions about the order in which the sources are activated.
 */
public class HybridSourceReader<T> implements SourceReader<T, HybridSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceReader.class);
    private final SourceReaderContext readerContext;
    private final List<SourceReader<T, ? extends SourceSplit>> chainedReaders;
    private final ArrayDeque<HybridSourceSplit> pendingSplits = new ArrayDeque<>();
    private int currentSourceIndex = -1;
    private boolean isFinalSource;
    private SourceReader<T, ? extends SourceSplit> currentReader;
    private CompletableFuture<Void> availabilityFuture;

    public HybridSourceReader(
            SourceReaderContext readerContext,
            List<SourceReader<T, ? extends SourceSplit>> readers) {
        this.readerContext = readerContext;
        this.chainedReaders = readers;
    }

    @Override
    public void start() {
        // underlying reader starts on demand with split assignment
    }

    @Override
    public InputStatus pollNext(ReaderOutput output) throws Exception {
        if (currentReader == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        InputStatus status = currentReader.pollNext(output);
        if (status == InputStatus.END_OF_INPUT) {
            // trap END_OF_INPUT unless all sources have finished
            LOG.info(
                    "End of input subtask={} sourceIndex={} {}",
                    readerContext.getIndexOfSubtask(),
                    currentSourceIndex,
                    currentReader);
            // Signal the coordinator that this reader has consumed all input and the
            // next source can potentially be activated.
            readerContext.sendSourceEventToCoordinator(
                    new SourceReaderFinishedEvent(currentSourceIndex));
            if (!isFinalSource) {
                // More splits may arrive for a subsequent reader.
                // InputStatus.NOTHING_AVAILABLE suspends poll, requires completion of the
                // availability future after receiving more splits to resume.
                return InputStatus.NOTHING_AVAILABLE;
            }
        }
        return status;
    }

    @Override
    public List<HybridSourceSplit> snapshotState(long checkpointId) {
        List<? extends SourceSplit> state = currentReader.snapshotState(checkpointId);
        return HybridSourceSplit.wrapSplits(currentSourceIndex, state);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // track future to resume reader after source switch
        if (currentReader != null) {
            return availabilityFuture = currentReader.isAvailable();
        } else {
            LOG.debug("Suspending pollNext due to no underlying reader");
            return availabilityFuture = new CompletableFuture<>();
        }
    }

    @Override
    public void addSplits(List<HybridSourceSplit> splits) {
        LOG.info(
                "Adding splits subtask={} sourceIndex={} currentReader={} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader,
                splits);
        List<SourceSplit> realSplits = new ArrayList<>(splits.size());
        for (HybridSourceSplit split : splits) {
            Preconditions.checkState(
                    split.sourceIndex() == currentSourceIndex,
                    "Split %s while current source is %s",
                    split,
                    currentSourceIndex);
            realSplits.add(split.getWrappedSplit());
        }
        currentReader.addSplits((List) realSplits);
    }

    @Override
    public void notifyNoMoreSplits() {
        if (currentReader != null) {
            currentReader.notifyNoMoreSplits();
        }
        LOG.debug(
                "No more splits for subtask={} sourceIndex={} currentReader={}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof SwitchSourceEvent) {
            SwitchSourceEvent sse = (SwitchSourceEvent) sourceEvent;
            LOG.info(
                    "Switch source event: subtask={} sourceIndex={}",
                    readerContext.getIndexOfSubtask(),
                    sse.sourceIndex());
            setCurrentReader(sse.sourceIndex());
            isFinalSource = sse.isFinalSource();
            if (availabilityFuture != null && !availabilityFuture.isDone()) {
                // continue polling
                availabilityFuture.complete(null);
            }
        } else {
            currentReader.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.close();
        }
        LOG.debug(
                "Reader closed: subtask={} sourceIndex={} currentReader={}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader);
    }

    private void setCurrentReader(int index) {
        Preconditions.checkArgument(index != currentSourceIndex);
        if (currentReader != null) {
            try {
                // TODO: retain snapshot splits for reset
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
        SourceReader<T, ?> reader = chainedReaders.get(index);
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
