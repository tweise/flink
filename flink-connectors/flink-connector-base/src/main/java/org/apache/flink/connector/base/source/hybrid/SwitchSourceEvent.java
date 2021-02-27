package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * Event sent from {@link HybridSplitEnumerator} to {@link HybridSourceReader} to switch to the
 * indicated reader.
 */
public class SwitchSourceEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;
    private final int sourceIndex;

    /**
     * Constructor.
     *
     * @param sourceIndex
     */
    public SwitchSourceEvent(int sourceIndex) {
        this.sourceIndex = sourceIndex;
    }

    public int sourceIndex() {
        return sourceIndex;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + '{' + "sourceIndex=" + sourceIndex + '}';
    }
}
