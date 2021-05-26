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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Wraps the actual split enumerators and facilitates source switching. Enumerators are created
 * lazily when source switch occurs to support runtime position conversion.
 *
 * <p>This enumerator delegates to the current underlying split enumerator and transitions to the
 * next source once all readers have indicated via {@link SourceReaderFinishedEvent} that all input
 * was consumed.
 *
 * <p>Switching between enumerators occurs by either creating the new enumerator with a fixed start
 * position via {@link Source#createEnumerator(SplitEnumeratorContext)} or by using the state of the
 * previous enumerator and the optional user supplied checkpoint state converter to create the next
 * enumerator with a runtime determined start position via {@link
 * Source#restoreEnumerator(SplitEnumeratorContext, Object)}.
 *
 * <p>During subtask recovery, splits that have been assigned since the last checkpoint will be
 * added back by the source coordinator. This requires the enumerator to potentially suspend the
 * current enumerator, activate previous readers and process the split backlog following the
 * original assignment before resuming with the current underlying enumerator.
 */
public class HybridSourceSplitEnumerator<SplitT extends SourceSplit>
        implements SplitEnumerator<HybridSourceSplit<SplitT>, HybridSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceSplitEnumerator.class);

    private final SplitEnumeratorContext<HybridSourceSplit> context;
    private final HybridSource.SourceChain<?, SplitT, Object> sourceChain;
    // TODO: SourceCoordinatorContext does not provide access to current assignments
    private final Map<Integer, List<HybridSourceSplit<SplitT>>> assignments;
    private final Map<Integer, TreeMap<Integer, List<HybridSourceSplit<SplitT>>>> pendingSplits;
    private final HashSet<Integer> pendingReaders;
    private int currentSourceIndex;
    private SplitEnumerator<SplitT, Object> currentEnumerator;

    public HybridSourceSplitEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> context,
            HybridSource.SourceChain<?, SplitT, Object> sourceChain) {
        this(context, sourceChain, 0);
    }

    public HybridSourceSplitEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> context,
            HybridSource.SourceChain<?, SplitT, Object> sourceChain,
            int initialSourceIndex) {
        Preconditions.checkArgument(initialSourceIndex < sourceChain.sources.size());
        this.context = context;
        this.sourceChain = sourceChain;
        this.currentSourceIndex = initialSourceIndex;
        this.assignments = new HashMap<>();
        this.pendingSplits = new HashMap<>();
        this.pendingReaders = new HashSet<>();
    }

    @Override
    public void start() {
        switchEnumerator();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOG.debug(
                "handleSplitRequest subtask={} sourceIndex={} pendingSplits={}",
                subtaskId,
                currentSourceIndex,
                pendingSplits);
        // TODO: test coverage for on demand split assignment
        assignPendingSplits(subtaskId);
        currentEnumerator.handleSplitRequest(subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<HybridSourceSplit<SplitT>> splits, int subtaskId) {
        LOG.debug("Adding splits back for subtask={} {}", subtaskId, splits);
        // Splits returned can belong to multiple sources, after switching since last checkpoint
        TreeMap<Integer, List<HybridSourceSplit<SplitT>>> splitsBySourceIndex = new TreeMap<>();

        for (HybridSourceSplit<SplitT> split : splits) {
            splitsBySourceIndex
                    .computeIfAbsent(split.sourceIndex(), k -> new ArrayList<>())
                    .add(split);
        }

        splitsBySourceIndex.forEach(
                (k, splitsPerSource) -> {
                    if (k == currentSourceIndex) {
                        currentEnumerator.addSplitsBack(
                                HybridSourceReader.unwrappedSplits(splitsPerSource), subtaskId);
                    } else {
                        pendingSplits
                                .computeIfAbsent(subtaskId, sourceIndex -> new TreeMap<>())
                                .put(k, splitsPerSource);
                        if (context.registeredReaders().containsKey(subtaskId)) {
                            assignPendingSplits(subtaskId);
                        }
                    }
                });
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("addReader subtaskId={}", subtaskId);
        if (pendingSplits.isEmpty()) {
            context.sendEventToSourceReader(subtaskId, new SwitchSourceEvent(currentSourceIndex));
            LOG.debug("Adding reader {} to enumerator {}", subtaskId, currentSourceIndex);
            currentEnumerator.addReader(subtaskId);
        } else {
            // Defer adding reader to the current enumerator until splits belonging to earlier
            // enumerators that were added back have been processed
            pendingReaders.add(subtaskId);
            assignPendingSplits(subtaskId);
        }
    }

    private void assignPendingSplits(int subtaskId) {
        TreeMap<Integer, List<HybridSourceSplit<SplitT>>> splitsBySource =
                pendingSplits.get(subtaskId);
        if (splitsBySource != null) {
            int sourceIndex = splitsBySource.firstKey();
            List<HybridSourceSplit<SplitT>> splits =
                    Preconditions.checkNotNull(splitsBySource.get(sourceIndex));
            LOG.debug("Assigning pending splits subtask={} {}", subtaskId, splits);
            context.sendEventToSourceReader(subtaskId, new SwitchSourceEvent(sourceIndex));
            context.assignSplits(
                    new SplitsAssignment<HybridSourceSplit>(
                            Collections.singletonMap(subtaskId, (List) splits)));
            context.signalNoMoreSplits(subtaskId);
            // Empty collection indicates that splits have been assigned
            splits.clear();
        }
    }

    @Override
    public HybridSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        Object enumState = currentEnumerator.snapshotState(checkpointId);
        return new HybridSourceEnumeratorState(currentSourceIndex, enumState);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceReaderFinishedEvent) {
            SourceReaderFinishedEvent srfe = (SourceReaderFinishedEvent) sourceEvent;
            if (srfe.sourceIndex() != currentSourceIndex) {
                if (srfe.sourceIndex() < currentSourceIndex) {
                    // Assign pending splits if any
                    TreeMap<Integer, List<HybridSourceSplit<SplitT>>> splitsBySource =
                            pendingSplits.get(subtaskId);
                    if (splitsBySource != null) {
                        List<HybridSourceSplit<SplitT>> splits =
                                splitsBySource.get(srfe.sourceIndex());
                        if (splits != null && splits.isEmpty()) {
                            // Splits have been processed by the reader
                            splitsBySource.remove(srfe.sourceIndex());
                        }
                        if (splitsBySource.isEmpty()) {
                            pendingSplits.remove(subtaskId);
                        } else {
                            Integer nextSubtaskSourceIndex = splitsBySource.firstKey();
                            LOG.debug(
                                    "Restore subtask={}, sourceIndex={}",
                                    subtaskId,
                                    nextSubtaskSourceIndex);
                            context.sendEventToSourceReader(
                                    subtaskId, new SwitchSourceEvent(nextSubtaskSourceIndex));
                            assignPendingSplits(subtaskId);
                        }
                    }
                    // Once all pending splits have been processed, add the readers to the current
                    // enumerator, which may in turn trigger new split assignments
                    if (!pendingReaders.isEmpty() && pendingSplits.isEmpty()) {
                        // Advance pending readers to current enumerator
                        LOG.debug(
                                "Adding pending readers {} to enumerator currentSourceIndex={}",
                                pendingReaders,
                                currentSourceIndex);
                        for (int pendingReaderSubtaskId : pendingReaders) {
                            context.sendEventToSourceReader(
                                    pendingReaderSubtaskId,
                                    new SwitchSourceEvent(currentSourceIndex));
                        }
                        for (int pendingReaderSubtaskId : pendingReaders) {
                            currentEnumerator.addReader(pendingReaderSubtaskId);
                        }
                        pendingReaders.clear();
                    }
                } else {
                    // enumerator already switched
                    LOG.debug("Ignoring out of order event {}", srfe);
                }
                return;
            }
            this.assignments.remove(subtaskId);
            LOG.info(
                    "Reader finished for subtask {} remaining assignments {}",
                    subtaskId,
                    assignments);
            if (this.assignments.isEmpty()) {
                LOG.debug("No assignments remaining, ready to switch source readers!");
                if (currentSourceIndex + 1 < sourceChain.sources.size()) {
                    switchEnumerator();
                    // switch all readers prior to sending split assignments
                    for (int i = 0; i < context.currentParallelism(); i++) {
                        context.sendEventToSourceReader(
                                i, new SwitchSourceEvent(currentSourceIndex));
                    }
                    // trigger split assignment,
                    // (initially happens as part of subtask/reader registration)
                    for (int i = 0; i < context.currentParallelism(); i++) {
                        LOG.debug("adding reader subtask={} sourceIndex={}", i, currentSourceIndex);
                        currentEnumerator.addReader(i);
                    }
                }
            }
        } else {
            currentEnumerator.handleSourceEvent(subtaskId, sourceEvent);
        }
    }

    @Override
    public void close() throws IOException {
        currentEnumerator.close();
    }

    private void switchEnumerator() {

        Object enumeratorState = null;
        if (currentEnumerator != null) {
            try {
                enumeratorState = currentEnumerator.snapshotState(-1);
                currentEnumerator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            currentEnumerator = null;
            currentSourceIndex++;
        }

        SplitEnumeratorContextProxy delegatingContext =
                new SplitEnumeratorContextProxy(currentSourceIndex, context, assignments);
        Source<?, ? extends SourceSplit, Object> source =
                (Source) sourceChain.sources.get(currentSourceIndex).f0;
        HybridSource.CheckpointConverter<Object, Object> converter =
                (HybridSource.CheckpointConverter) sourceChain.sources.get(currentSourceIndex).f1;
        try {
            if (converter != null) {
                currentEnumerator =
                        source.restoreEnumerator(
                                delegatingContext, converter.apply(enumeratorState));
            } else {
                currentEnumerator = source.createEnumerator(delegatingContext);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create enumerator for sourceIndex=" + currentSourceIndex, e);
        }
        LOG.info("Starting enumerator for sourceIndex={}", currentSourceIndex);
        currentEnumerator.start();
    }

    /**
     * The {@link SplitEnumeratorContext} that is provided to the currently active enumerator.
     *
     * <p>This context is used to wrap the splits into {@link HybridSourceSplit} and track
     * assignment to readers.
     */
    private static class SplitEnumeratorContextProxy<SplitT extends SourceSplit>
            implements SplitEnumeratorContext<SplitT> {
        private static final Logger LOG =
                LoggerFactory.getLogger(SplitEnumeratorContextProxy.class);

        private final SplitEnumeratorContext<HybridSourceSplit<?>> realContext;
        private final int sourceIndex;
        // TODO: SourceCoordinatorContext does not provide access to current assignments
        private final Map<Integer, List<HybridSourceSplit<?>>> assignments;

        public SplitEnumeratorContextProxy(
                int sourceIndex,
                SplitEnumeratorContext<HybridSourceSplit<?>> realContext,
                Map<Integer, List<HybridSourceSplit<?>>> assignments) {
            this.realContext = realContext;
            this.sourceIndex = sourceIndex;
            this.assignments = assignments;
        }

        @Override
        public MetricGroup metricGroup() {
            return realContext.metricGroup();
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            realContext.sendEventToSourceReader(subtaskId, event);
        }

        @Override
        public int currentParallelism() {
            return realContext.currentParallelism();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return realContext.registeredReaders();
        }

        @Override
        public void assignSplits(SplitsAssignment<SplitT> newSplitAssignments) {
            Map<Integer, List<HybridSourceSplit<?>>> wrappedAssignmentMap = new HashMap<>();
            for (Map.Entry<Integer, List<SplitT>> e : newSplitAssignments.assignment().entrySet()) {
                List<HybridSourceSplit<?>> splits =
                        HybridSourceReader.wrappedSplits(sourceIndex, e.getValue());
                wrappedAssignmentMap.put(e.getKey(), splits);
                assignments.merge(
                        e.getKey(),
                        splits,
                        (all, plus) -> {
                            all.addAll(plus);
                            return all;
                        });
            }
            SplitsAssignment<HybridSourceSplit<?>> wrappedAssignments =
                    new SplitsAssignment<>(wrappedAssignmentMap);
            LOG.debug("Assigning splits sourceIndex={} {}", sourceIndex, wrappedAssignments);
            realContext.assignSplits(wrappedAssignments);
        }

        @Override
        public void assignSplit(SplitT split, int subtask) {
            HybridSourceSplit<SplitT> wrappedSplit = new HybridSourceSplit(sourceIndex, split);
            assignments.merge(
                    subtask,
                    new ArrayList<>(Arrays.asList(wrappedSplit)),
                    (all, plus) -> {
                        all.addAll(plus);
                        return all;
                    });
            realContext.assignSplit(wrappedSplit, subtask);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            realContext.signalNoMoreSplits(subtask);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            realContext.callAsync(callable, handler);
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {
            realContext.callAsync(callable, handler, initialDelay, period);
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            realContext.runInCoordinatorThread(runnable);
        }
    }
}
