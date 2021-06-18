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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hybrid source that switches underlying sources based on configured source chain.
 *
 * <pre>{@code
 * FileSource<String> fileSource = null;
 * KafkaSource<String> kafkaSource = null;
 * HybridSource<String> hybridSource =
 *     new HybridSourceBuilder<String, ContinuousFileSplitEnumerator>(fileSource)
 *         .addSource(
 *             kafkaSource,
 *             (source, enumerator) -> {
 *               // customize Kafka source here
 *               return source;
 *             })
 *         .build();
 * }</pre>
 */
@PublicEvolving
public class HybridSource<T> implements Source<T, HybridSourceSplit, HybridSourceEnumeratorState> {

    private final List<SourceListEntry> sources;
    // sources are populated per subtask at switch time
    private final Map<Integer, Source> switchedSources;

    /** Protected for subclass, use {@link #builder(Source)} to construct source. */
    protected HybridSource(List<SourceListEntry> sources) {
        Preconditions.checkArgument(!sources.isEmpty());
        for (int i = 0; i < sources.size() - 1; i++) {
            Preconditions.checkArgument(
                    Boundedness.BOUNDED.equals(sources.get(i).source.getBoundedness()),
                    "All sources except the final source need to be bounded.");
        }
        this.sources = sources;
        this.switchedSources = new HashMap<>(sources.size());
    }

    /** Builder for {@link HybridSource}. */
    public static <T, EnumT extends SplitEnumerator> HybridSourceBuilder<T, EnumT> builder(
            Source<T, ?, ?> firstSource) {
        return new HybridSourceBuilder<>(firstSource);
    }

    @Override
    public Boundedness getBoundedness() {
        return sources.get(sources.size() - 1).source.getBoundedness();
    }

    @Override
    public SourceReader<T, HybridSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // List<SourceReader<T, ? extends SourceSplit>> readers = new ArrayList<>();
        // for (SourceListEntry source : sources) {
        //    readers.add(source.source.createReader(readerContext));
        // }
        return new HybridSourceReader(readerContext, switchedSources);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext) {
        return new HybridSourceSplitEnumerator(enumContext, sources, 0, switchedSources);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext,
            HybridSourceEnumeratorState checkpoint)
            throws Exception {
        // TODO: restore underlying enumerator
        return new HybridSourceSplitEnumerator(
                enumContext, sources, checkpoint.getCurrentSourceIndex(), switchedSources);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        // List<SimpleVersionedSerializer<SourceSplit>> serializers = new ArrayList<>();
        // TODO: serializers are created on demand as underlying sources are created during switch
        // sources.forEach(t -> serializers.add(castSerializer(t.source.getSplitSerializer())));
        return new HybridSourceSplitSerializer(switchedSources);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        List<SimpleVersionedSerializer<Object>> serializers = new ArrayList<>();
        sources.forEach(
                t -> serializers.add(castSerializer(t.source.getEnumeratorCheckpointSerializer())));
        return new HybridSourceEnumeratorStateSerializer(serializers);
    }

    private static <T> SimpleVersionedSerializer<T> castSerializer(
            SimpleVersionedSerializer<? extends T> s) {
        @SuppressWarnings("rawtypes")
        SimpleVersionedSerializer s1 = s;
        return s1;
    }

    /**
     * Callback for switch time customization of the underlying source, typically to dynamically set
     * a start position from previous enumerator end state.
     *
     * <p>Requires the ability to augment the existing source (or clone and modify). Provides the
     * flexibility to set start position in any way a source allows, in a source specific way.
     * Future convenience could be built on top of it, for example an implementation recognizes
     * optional interfaces.
     *
     * <p>Called when the current enumerator has finished and before the next enumerator is created.
     * The enumerator end state can thus be used to set the next source's start start position.
     *
     * <p>Only required for dynamic position transfer at time of switching, otherwise source can be
     * preconfigured with a start position during job submission.
     */
    public interface SourceConfigurer<SourceT extends Source, FromEnumT extends SplitEnumerator>
            extends Serializable {
        SourceT configure(SourceT source, FromEnumT enumerator);
    }

    private static class NoopSourceConfigurer<
                    SourceT extends Source, FromEnumT extends SplitEnumerator>
            implements SourceConfigurer<SourceT, FromEnumT> {
        @Override
        public SourceT configure(SourceT source, FromEnumT enumerator) {
            return source;
        }
    }

    /** Entry for list of underlying sources. */
    protected static class SourceListEntry implements Serializable {
        protected final Source source;
        protected final SourceConfigurer configurer;

        private SourceListEntry(Source source, SourceConfigurer configurer) {
            this.source = source;
            this.configurer = configurer;
        }

        public static SourceListEntry of(Source source, SourceConfigurer configurer) {
            return new SourceListEntry(source, configurer);
        }
    }

    /** Builder for HybridSource. */
    public static class HybridSourceBuilder<T, EnumT extends SplitEnumerator>
            implements Serializable {
        private final List<SourceListEntry> sources;

        public HybridSourceBuilder(Source<T, ?, ?> initialSource) {
            this(concat(Collections.emptyList(), SourceListEntry.of(initialSource, null)));
        }

        private HybridSourceBuilder(List<SourceListEntry> sources) {
            this.sources = sources;
        }

        /** Add source without switch time modification. */
        public <ToEnumT extends SplitEnumerator, NextSourceT extends Source<T, ?, ?>>
                HybridSourceBuilder<T, ToEnumT> addSource(NextSourceT source) {
            return addSource(source, new NoopSourceConfigurer<>());
        }

        /** Add source with start position conversion from previous enumerator. */
        public <ToEnumT extends SplitEnumerator, NextSourceT extends Source<T, ?, ?>>
                HybridSourceBuilder<T, ToEnumT> addSource(
                        NextSourceT source, SourceConfigurer<NextSourceT, EnumT> sourceConfigurer) {
            return new HybridSourceBuilder<>(
                    concat(this.sources, SourceListEntry.of(source, sourceConfigurer)));
        }

        /** Build the source. */
        public HybridSource<T> build() {
            return new HybridSource(sources);
        }

        private static <T> List<T> concat(List<T> src, T element) {
            List<T> result = new ArrayList<>(src);
            result.add(element);
            return result;
        }
    }
}
