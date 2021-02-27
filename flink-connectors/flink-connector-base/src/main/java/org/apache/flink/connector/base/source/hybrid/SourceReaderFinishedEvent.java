package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * A source event sent from the HybridSourceReader to the enumerator to indicate that the current
 * reader has finished and splits for the next reader can be sent.
 */
public class SourceReaderFinishedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;
    private final int sourceIndex;

    /**
     * Constructor.
     *
     * @param sourceIndex
     */
    public SourceReaderFinishedEvent(int sourceIndex) {
        this.sourceIndex = sourceIndex;
    }

    public int sourceIndex() {
        return sourceIndex;
    }

    @Override
    public String toString() {
        return "SourceReaderFinishedEvent{" + "sourceIndex=" + sourceIndex + '}';
    }
}
