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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** IT case for the {@link Source} with a coordinator. */
public class CoordinatedSourceITCase extends AbstractTestBase {

    @Test
    public void testHybridSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HybridSource.SourceChain<Integer, MockSourceSplit, List<MockSourceSplit>> sourceChain;
        boolean withSourceStartPosition = false;
        if (withSourceStartPosition) {
            // static start position, which should be the more common approach
            sourceChain =
                    HybridSource.SourceChain.of(
                            new MockBaseSource(2, 10, Boundedness.BOUNDED),
                            new MockBaseSource(2, 10, 20, Boundedness.BOUNDED));
        } else {
            // position conversion at switch time
            // start position can be derived from previous enumerator state
            sourceChain =
                    new HybridSource.SourceChain<>(new MockBaseSource(2, 10, Boundedness.BOUNDED));
            sourceChain =
                    sourceChain.add(
                            new MockBaseSource(1, 1, Boundedness.BOUNDED),
                            (mockSourceSplits -> {
                                int numSplits = 2;
                                int numRecordsPerSplit = 10;
                                int startingValue = 20;
                                Boundedness boundedness = Boundedness.BOUNDED;
                                // TODO: convert source parameters to splits from
                                // MockBaseSource.createEnumerator - this should be a
                                // shared utility
                                List<MockSourceSplit> splits = new ArrayList<>();
                                for (int i = 0; i < numSplits; i++) {
                                    int endIndex =
                                            boundedness == Boundedness.BOUNDED
                                                    ? numRecordsPerSplit
                                                    : Integer.MAX_VALUE;
                                    MockSourceSplit split = new MockSourceSplit(i, 0, endIndex);
                                    for (int j = 0; j < numRecordsPerSplit; j++) {
                                        split.addRecord(startingValue + i * numRecordsPerSplit + j);
                                    }
                                    splits.add(split);
                                }
                                return splits;
                            }));
        }
        Source source = new HybridSource<>(sourceChain);
        DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource")
                        .returns(Integer.class);
        // env.setParallelism(1);
        executeAndVerify(env, stream, 40);
    }

    @Test
    public void testEnumeratorReaderCommunication() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockBaseSource source = new MockBaseSource(2, 10, Boundedness.BOUNDED);
        DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @Test
    public void testMultipleSources() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockBaseSource source1 = new MockBaseSource(2, 10, Boundedness.BOUNDED);
        MockBaseSource source2 = new MockBaseSource(2, 10, 20, Boundedness.BOUNDED);
        DataStream<Integer> stream1 =
                env.fromSource(source1, WatermarkStrategy.noWatermarks(), "TestingSource1");
        DataStream<Integer> stream2 =
                env.fromSource(source2, WatermarkStrategy.noWatermarks(), "TestingSource2");
        executeAndVerify(env, stream1.union(stream2), 40);
    }

    @SuppressWarnings("serial")
    private void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<Integer> stream, int numRecords)
            throws Exception {
        stream.addSink(
                new RichSinkFunction<Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<Integer>());
                    }

                    @Override
                    public void invoke(Integer value, Context context) throws Exception {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });
        List<Integer> result = env.execute().getAccumulatorResult("result");
        Collections.sort(result);
        assertEquals(numRecords, result.size());
        assertEquals(0, (int) result.get(0));
        assertEquals(numRecords - 1, (int) result.get(result.size() - 1));
    }
}
