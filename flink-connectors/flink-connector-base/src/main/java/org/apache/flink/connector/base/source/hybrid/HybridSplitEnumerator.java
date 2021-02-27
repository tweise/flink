package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Wraps the actual split enumerators and facilitates source switching. Enumerators are created
 * lazily when source switch occurs to support runtime position conversion.
 *
 * @param <SplitT>
 * @param <CheckpointT>
 */
public class HybridSplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        implements SplitEnumerator<HybridSourceSplit<SplitT>, List<HybridSourceSplit<SplitT>>> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSplitEnumerator.class);

    private final SplitEnumeratorContext<HybridSourceSplit> context;
    private final HybridSource.SourceChain<?, SplitT, Object> sourceChain;
    // TODO: SourceCoordinatorContext does not provide access to current assignments
    private final Map<Integer, List<HybridSourceSplit<SplitT>>> assignments;
    private int currentSourceIndex;
    private SplitEnumerator<SplitT, Object> currentEnumerator;

    public HybridSplitEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> context,
            HybridSource.SourceChain<?, SplitT, Object> sourceChain) {
        this.context = context;
        this.sourceChain = sourceChain;
        this.currentSourceIndex = 0;
        this.assignments = new HashMap<>();
    }

    @Override
    public void start() {
        switchEnumerator();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        currentEnumerator.handleSplitRequest(subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<HybridSourceSplit<SplitT>> splits, int subtaskId) {
        currentEnumerator.addSplitsBack(HybridSourceReader.unwrappedSplits(splits), subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        currentEnumerator.addReader(subtaskId);
    }

    @Override
    public List<HybridSourceSplit<SplitT>> snapshotState() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceReaderFinishedEvent) {
            SourceReaderFinishedEvent srfe = (SourceReaderFinishedEvent) sourceEvent;
            if (srfe.sourceIndex() != currentSourceIndex) {
                // source already switched
                LOG.debug("Ignoring out of order source event {}", srfe);
                return;
            }
            this.assignments.remove(subtaskId);
            LOG.info(
                    "Reader finished for subtask {} remaining assignments {}",
                    subtaskId,
                    assignments);
            if (this.assignments.isEmpty()) {
                LOG.debug("No assignments remaining, ready to switch source!");
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
                enumeratorState = currentEnumerator.snapshotState();
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
     * The enumerator context that is provided to the currently active enumerator by the hybrid
     * source.
     *
     * @param <SplitT>
     */
    private static class SplitEnumeratorContextProxy<SplitT extends SourceSplit>
            implements SplitEnumeratorContext<SplitT> {
        private static final Logger LOG =
                LoggerFactory.getLogger(SplitEnumeratorContextProxy.class);

        private final SplitEnumeratorContext<HybridSourceSplit<SplitT>> realContext;
        private final int sourceIndex;
        // TODO: SourceCoordinatorContext does not provide access to current assignments
        private final Map<Integer, List<HybridSourceSplit<SplitT>>> assignments;

        public SplitEnumeratorContextProxy(
                int sourceIndex,
                SplitEnumeratorContext<HybridSourceSplit<SplitT>> realContext,
                Map<Integer, List<HybridSourceSplit<SplitT>>> assignments) {
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
            Map<Integer, List<HybridSourceSplit<SplitT>>> wrappedAssignmentMap = new HashMap<>();
            for (Map.Entry<Integer, List<SplitT>> e : newSplitAssignments.assignment().entrySet()) {
                List<HybridSourceSplit<SplitT>> splits =
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
            SplitsAssignment<HybridSourceSplit<SplitT>> wrappedAssignments =
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
