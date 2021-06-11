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
 * SourceReaderFinishedEvent} to the coordinator and waits for the {@link SwitchSourceEvent}.
 */
public class HybridSourceReader<T> implements SourceReader<T, HybridSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceReader.class);
    private final SourceReaderContext readerContext;
    private final List<SourceReader<T, ? extends SourceSplit>> chainedReaders;
    private final ArrayDeque<HybridSourceSplit> pendingSplits = new ArrayDeque<>();
    private int currentSourceIndex = -1;
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
            if (pendingSplits.isEmpty()) {
                // no underlying reader before split assignment
                return InputStatus.NOTHING_AVAILABLE;
            } else {
                setCurrentReader(pendingSplits.peek().sourceIndex());
            }
        }

        InputStatus status = currentReader.pollNext(output);
        if (status == InputStatus.END_OF_INPUT) {
            // trap END_OF_INPUT if this wasn't the final reader
            LOG.info(
                    "End of input subtask={} sourceIndex={} {}",
                    readerContext.getIndexOfSubtask(),
                    currentSourceIndex,
                    currentReader);
            if (currentSourceIndex + 1 < chainedReaders.size()) {
                // Signal the coordinator that the current reader has consumed all input and the
                // next source can potentially be activated (after all readers are ready).
                readerContext.sendSourceEventToCoordinator(
                        new SourceReaderFinishedEvent(currentSourceIndex));
                if (!pendingSplits.isEmpty()) {
                    // we have splits for another reader waiting
                    setCurrentReader(pendingSplits.peek().sourceIndex());
                    return InputStatus.MORE_AVAILABLE;
                }
                // More splits may arrive for this or subsequent reader.
                // InputStatus.NOTHING_AVAILABLE requires us to complete the availability
                // future after receiving more splits to resume poll.
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
            LOG.debug("Suspend pollNext due to no active reader");
            return availabilityFuture = new CompletableFuture<>();
        }
    }

    @Override
    public void addSplits(List<HybridSourceSplit> splits) {
        LOG.info(
                "Receiving splits subtask={} sourceIndex={} currentReader={} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader,
                splits);
        pendingSplits.addAll(splits);
        if (availabilityFuture != null && !availabilityFuture.isDone()) {
            // continue polling
            LOG.debug("Resuming pollNext");
            availabilityFuture.complete(null);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        currentReader.notifyNoMoreSplits();
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
            // TODO: remove SwitchSourceEvent as reader switch is now driven by assigned splits
            // setCurrentReader(sse.sourceIndex());
            // if (availabilityFuture != null && !availabilityFuture.isDone()) {
            //    // continue polling
            //    availabilityFuture.complete(null);
            // }
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
        Preconditions.checkState(
                currentSourceIndex <= index, "reader index monotonically increasing");
        Preconditions.checkState(index < chainedReaders.size(), "invalid reader index: %s", index);
        if (currentSourceIndex == index) {
            LOG.debug(
                    "Reader already set to process source: subtask={} sourceIndex={} currentReader={}",
                    readerContext.getIndexOfSubtask(),
                    currentSourceIndex,
                    currentReader);
            return;
        }
        if (currentReader != null) {
            try {
                // TODO: retain splits for reset
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

        // assign waiting splits, if any
        List<SourceSplit> splits = new ArrayList<>(pendingSplits.size());
        HybridSourceSplit hybridSplit;
        while ((hybridSplit = pendingSplits.peek()) != null) {
            if (hybridSplit.sourceIndex() == currentSourceIndex) {
                splits.add(pendingSplits.removeFirst().getWrappedSplit());
            } else {
                // no more splits for this reader
                break;
            }
        }
        LOG.info(
                "Adding splits subtask={} sourceIndex={} currentReader={} {}",
                readerContext.getIndexOfSubtask(),
                currentSourceIndex,
                currentReader,
                splits);
        currentReader.addSplits((List) splits);
    }
}
