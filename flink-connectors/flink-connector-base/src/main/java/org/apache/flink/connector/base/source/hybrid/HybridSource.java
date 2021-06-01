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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** Hybrid source that switches underlying sources based on configured source chain. */
@PublicEvolving
public class HybridSource<T> implements Source<T, HybridSourceSplit, HybridSourceEnumeratorState> {

    private final SourceChain<T, ?> sourceChain;

    public HybridSource(SourceChain<T, ?> sourceChain) {
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
        List<SourceReader<T, ? extends SourceSplit>> readers = new ArrayList<>();
        for (Tuple2<Source<T, ? extends SourceSplit, ?>, ?> source : sourceChain.sources) {
            readers.add(source.f0.createReader(readerContext));
        }
        return new HybridSourceReader(readerContext, readers);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext) {
        return new HybridSourceSplitEnumerator(enumContext, sourceChain);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext,
            HybridSourceEnumeratorState checkpoint)
            throws Exception {
        return new HybridSourceSplitEnumerator(
                enumContext, sourceChain, checkpoint.getCurrentSourceIndex());
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        List<SimpleVersionedSerializer<SourceSplit>> serializers = new ArrayList<>();
        sourceChain.sources.forEach(
                t -> serializers.add(castSerializer(t.f0.getSplitSerializer())));
        return new HybridSourceSplitSerializer(serializers);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        List<SimpleVersionedSerializer<Object>> serializers = new ArrayList<>();
        sourceChain.sources.forEach(
                t -> serializers.add(castSerializer(t.f0.getEnumeratorCheckpointSerializer())));
        return new HybridSourceEnumeratorStateSerializer(serializers);
    }

    private static <T> SimpleVersionedSerializer<T> castSerializer(
            SimpleVersionedSerializer<? extends T> s) {
        @SuppressWarnings("rawtypes")
        SimpleVersionedSerializer s1 = s;
        return s1;
    }

    /**
     * Converts checkpoint between sources to transfer end position to next source's start position.
     * Only required for dynamic position transfer at time of switching, otherwise source can be
     * preconfigured with a start position during job submission.
     */
    public interface CheckpointConverter<InCheckpointT, OutCheckpointT>
            extends Function<InCheckpointT, OutCheckpointT>, Serializable {}

    /** Chain of sources with option to convert start position at switch-time. */
    public static class SourceChain<T, EnumChkT> implements Serializable {
        final List<Tuple2<Source<T, ? extends SourceSplit, ?>, CheckpointConverter<?, ?>>> sources;

        public SourceChain(Source<T, ?, EnumChkT> initialSource) {
            this(concat(Collections.emptyList(), Tuple2.of(initialSource, null)));
        }

        /** Construct a chain of homogeneous sources with fixed start position. */
        public static <T, EnumChkT> SourceChain<T, EnumChkT> of(Source<T, ?, EnumChkT>... sources) {
            Preconditions.checkArgument(sources.length >= 1, "At least one source");
            SourceChain<T, EnumChkT> sourceChain = new SourceChain<>(sources[0]);
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
        public <NextEnumChkT> SourceChain<T, NextEnumChkT> add(
                Source<T, ?, NextEnumChkT> nextSource) {
            return add(nextSource, null);
        }

        /** Add source with start position conversion from previous source. */
        public <NextEnumChkT> SourceChain<T, NextEnumChkT> add(
                Source<T, ?, NextEnumChkT> nextSource,
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
