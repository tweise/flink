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

import org.apache.flink.annotation.PublicEvolving;
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

/** Hybrid source that switches underlying sources based on configurable source chain. */
@PublicEvolving
public class HybridSource<T> implements Source<T, HybridSourceSplit, HybridSourceEnumState> {

    private final SourceChain<T, ? extends SourceSplit, ?> sourceChain;

    public HybridSource(SourceChain<T, ? extends SourceSplit, ?> sourceChain) {
        Preconditions.checkArgument(!sourceChain.sources.isEmpty());
        for (int i = 0; i < sourceChain.sources.size() - 1; i++) {
            Preconditions.checkArgument(
                    Boundedness.BOUNDED.equals(sourceChain.sources.get(i).f0.getBoundedness()),
                    "All sources except the final source need to be bounded.");
        }
        this.sourceChain = sourceChain;
    }

    @Override
    public Boundedness getBoundedness() {
        return sourceChain.sources.get(sourceChain.sources.size() - 1).f0.getBoundedness();
    }

    @Override
    public SourceReader<T, HybridSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        List<SourceReader<?, ? extends SourceSplit>> readers = new ArrayList<>();
        for (Tuple2<Source<T, ? extends SourceSplit, ?>, ?> source : sourceChain.sources) {
            readers.add(source.f0.createReader(readerContext));
        }
        return new HybridSourceReader(readerContext, readers);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumState> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext) {
        return new HybridSourceSplitEnumerator(enumContext, sourceChain);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext, HybridSourceEnumState checkpoint)
            throws Exception {
        return new HybridSourceSplitEnumerator(
                enumContext, sourceChain, checkpoint.getCurrentSourceIndex());
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        List<SimpleVersionedSerializer<SourceSplit>> serializers = new ArrayList<>();
        sourceChain.sources.forEach(
                t -> serializers.add(castSerializer(t.f0.getSplitSerializer())));
        return new SplitSerializerWrapper<>(serializers);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceEnumState> getEnumeratorCheckpointSerializer() {
        List<SimpleVersionedSerializer<Object>> serializers = new ArrayList<>();
        sourceChain.sources.forEach(
                t -> serializers.add(castSerializer(t.f0.getEnumeratorCheckpointSerializer())));
        return new HybridSourceEnumStateSerializer(serializers);
    }

    private static <T> SimpleVersionedSerializer<T> castSerializer(
            SimpleVersionedSerializer<? extends T> s) {
        @SuppressWarnings("rawtypes")
        SimpleVersionedSerializer s1 = s;
        return s1;
    }

    /** Serializes splits by delegating to the source-indexed split serializer. */
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

    /**
     * Converts checkpoint between sources to transfer end position to next source's start position.
     * Only required for dynamic position transfer at time of switching, otherwise source can be
     * preconfigured with a start position during job submission.
     */
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

        /** Add source with fixed start position. */
        public <NextSplitT extends SourceSplit, NextEnumChkT>
                SourceChain<T, NextSplitT, NextEnumChkT> add(
                        Source<T, NextSplitT, NextEnumChkT> nextSource) {
            return add(nextSource, null);
        }

        /** Add source with start position conversion from previous source. */
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
