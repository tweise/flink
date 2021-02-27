package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.SourceSplit;

public class HybridSourceSplit<SplitT extends SourceSplit> implements SourceSplit {

    private final SplitT realSplit;
    private final int sourceIndex;

    public HybridSourceSplit(int sourceIndex, SplitT realSplit) {
        this.sourceIndex = sourceIndex;
        this.realSplit = realSplit;
    }

    public int sourceIndex() {
        return this.sourceIndex;
    }

    public SplitT getWrappedSplit() {
        return realSplit;
    }

    @Override
    public String splitId() {
        return realSplit.splitId();
    }

    @Override
    public String toString() {
        return "HybridSourceSplit{"
                + "realSplit="
                + realSplit
                + ", sourceIndex="
                + sourceIndex
                + '}';
    }
}
