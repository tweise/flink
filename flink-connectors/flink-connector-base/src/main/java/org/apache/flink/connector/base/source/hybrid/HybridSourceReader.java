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

public class HybridSourceReader<T, SplitT extends SourceSplit>
        implements SourceReader<T, HybridSourceSplit<SplitT>> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceReader.class);
    private SourceReaderContext readerContext;
    private List<SourceReader<?, SplitT>> realReaders;
    private int currentSourceIndex;
    private SourceReader<?, SplitT> currentReader;

    // TODO: defer reader creation by sending the source with SwitchSourceEvent?
    public HybridSourceReader(
            SourceReaderContext readerContext,
            List<SourceReader<?, SplitT>> readers,
            int currentSourceIndex) {
        this.readerContext = readerContext;
        this.realReaders = readers;
        this.currentSourceIndex = currentSourceIndex;
    }

    @Override
    public void start() {
        setCurrentReader(currentSourceIndex);
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
                readerContext.sendSourceEventToCoordinator(
                        new SourceReaderFinishedEvent(currentSourceIndex));
                // more data will be available from the next reader
                return InputStatus.MORE_AVAILABLE;
            }
        }
        return status;
    }

    @Override
    public List<HybridSourceSplit<SplitT>> snapshotState(long checkpointId) {
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
        LOG.debug(
                "Adding splits subtask={} sourceIndex={} {} {}",
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
