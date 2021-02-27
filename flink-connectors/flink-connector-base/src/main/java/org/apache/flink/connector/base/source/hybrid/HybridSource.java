package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Hybrid source that switches underlying sources based on configurable source chain.
 *
 * @param <T>
 */
public class HybridSource<T> implements Source<T, HybridSourceSplit, List<HybridSourceSplit>> {

    private final SourceChain<T, ? extends SourceSplit, ?> sourceChain;

    public HybridSource(SourceChain<T, ? extends SourceSplit, ?> sourceChain) {
        this.sourceChain = sourceChain;
    }

    @Override
    public Boundedness getBoundedness() {
        for (Tuple2<Source<T, ? extends SourceSplit, ?>, ?> t : sourceChain.sources) {
            if (t.f0.getBoundedness() == Boundedness.CONTINUOUS_UNBOUNDED) {
                return Boundedness.CONTINUOUS_UNBOUNDED;
            }
        }
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, HybridSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        List<SourceReader<?, ? extends SourceSplit>> readers = new ArrayList<>();
        for (Tuple2<Source<T, ? extends SourceSplit, ?>, ?> source : sourceChain.sources) {
            readers.add(source.f0.createReader(readerContext));
        }
        return new HybridSourceReader(readerContext, readers, 0);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, List<HybridSourceSplit>> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext) {
        return new HybridSplitEnumerator(enumContext, sourceChain);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, List<HybridSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext,
            List<HybridSourceSplit> checkpoint)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        List<SimpleVersionedSerializer<SourceSplit>> serializers = new ArrayList<>();
        sourceChain.sources.forEach(
                t -> serializers.add((SimpleVersionedSerializer) t.f0.getSplitSerializer()));
        return new SplitSerializerWrapper<>(serializers);
    }

    @Override
    public SimpleVersionedSerializer<List<HybridSourceSplit>> getEnumeratorCheckpointSerializer() {
        return new EnumeratorCheckpointSerializer();
    }

    /**
     * Serializes splits by delegating to the source-indexed split serializer
     *
     * @param <SplitT>
     */
    public static class SplitSerializerWrapper<SplitT extends SourceSplit>
            implements SimpleVersionedSerializer<HybridSourceSplit> {

        final List<SimpleVersionedSerializer<SourceSplit>> serializers;

        public SplitSerializerWrapper(List<SimpleVersionedSerializer<SourceSplit>> serializers) {
            this.serializers = serializers;
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(HybridSourceSplit split) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                out.writeInt(split.sourceIndex());
                out.writeInt(serializerOf(split.sourceIndex()).getVersion());
                byte[] serializedSplit =
                        serializerOf(split.sourceIndex()).serialize(split.getWrappedSplit());
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
                return baos.toByteArray();
            }
        }

        @Override
        public HybridSourceSplit deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                int sourceIndex = in.readInt();
                int nestedVersion = in.readInt();
                int length = in.readInt();
                byte[] splitBytes = new byte[length];
                in.readFully(splitBytes);
                SourceSplit split =
                        serializerOf(sourceIndex).deserialize(nestedVersion, splitBytes);
                return new HybridSourceSplit(sourceIndex, split);
            }
        }

        private SimpleVersionedSerializer<SourceSplit> serializerOf(int sourceIndex) {
            Preconditions.checkArgument(sourceIndex < serializers.size());
            return serializers.get(sourceIndex);
        }
    }

    public static class EnumeratorCheckpointSerializer
            implements SimpleVersionedSerializer<List<HybridSourceSplit>> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(List<HybridSourceSplit> obj) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HybridSourceSplit> deserialize(int version, byte[] serialized)
                throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Converts checkpoint between sources to transfer end position to next source's start position.
     * Only required for dynamic position transfer at time of switching, otherwise source can be
     * preconfigured with a start position during job submission.
     */
    // TODO: Maybe provide old enumerator as input?
    public interface CheckpointConverter<InCheckpointT, OutCheckpointT>
            extends Function<InCheckpointT, OutCheckpointT>, Serializable {}

    /** Chain of sources with option to convert start position at switch-time. */
    public static class SourceChain<T, SplitT extends SourceSplit, EnumChkT>
            implements Serializable {
        final List<Tuple2<Source<T, ? extends SourceSplit, ?>, CheckpointConverter<?, ?>>> sources;

        public SourceChain(Source<T, SplitT, EnumChkT> initialSource) {
            this(concat(Collections.emptyList(), Tuple2.of(initialSource, null)));
        }

        public static <T, SplitT extends SourceSplit, EnumChkT> SourceChain<T, SplitT, EnumChkT> of(
                Source<T, SplitT, EnumChkT>... sources) {
            Preconditions.checkArgument(sources.length >= 1, "At least one source");
            SourceChain<T, SplitT, EnumChkT> sourceChain = new SourceChain<>(sources[0]);
            for (int i = 1; i < sources.length; i++) {
                sourceChain = sourceChain.add(sources[i]);
            }
            return sourceChain;
        }

        private SourceChain(
                List<Tuple2<Source<T, ? extends SourceSplit, ?>, CheckpointConverter<?, ?>>>
                        sources) {
            this.sources = sources;
        }

        /**
         * Add source with fixed start position.
         *
         * @param nextSource
         * @param <NextSplitT>
         * @param <NextEnumChkT>
         * @return
         */
        public <NextSplitT extends SourceSplit, NextEnumChkT>
                SourceChain<T, NextSplitT, NextEnumChkT> add(
                        Source<T, NextSplitT, NextEnumChkT> nextSource) {
            return add(nextSource, null);
        }

        /**
         * Add source with start position conversion from previous source.
         *
         * @param nextSource
         * @param converter
         * @param <NextSplitT>
         * @param <NextEnumChkT>
         * @return
         */
        public <NextSplitT extends SourceSplit, NextEnumChkT>
                SourceChain<T, NextSplitT, NextEnumChkT> add(
                        Source<T, NextSplitT, NextEnumChkT> nextSource,
                        CheckpointConverter<EnumChkT, NextEnumChkT> converter) {
            return new SourceChain<>(concat(this.sources, Tuple2.of(nextSource, converter)));
        }

        private static <T> List<T> concat(List<T> src, T element) {
            List<T> result = new ArrayList<>(src);
            result.add(element);
            return result;
        }
    }
}
