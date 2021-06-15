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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.mock.Whitebox;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/** Tests for {@link HybridSourceReader}. */
public class HybridSourceReaderTest {

    @Test
    public void testReader() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
        MockBaseSource source = new MockBaseSource(1, 1, Boundedness.BOUNDED);
        // 2 underlying readers to exercise switch
        SourceReader<Integer, MockSourceSplit> mockSplitReader1 =
                source.createReader(readerContext);
        SourceReader<Integer, MockSourceSplit> mockSplitReader2 =
                source.createReader(readerContext);
        HybridSourceReader<Integer> reader =
                new HybridSourceReader<>(
                        readerContext, Arrays.asList(mockSplitReader1, mockSplitReader2));

        reader.start();

        Assert.assertNull(currentReader(reader));
        Assert.assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(readerOutput));

        reader.handleSourceEvents(new SwitchSourceEvent(0, false));
        MockSourceSplit mockSplit = new MockSourceSplit(0, 0, 1);
        mockSplit.addRecord(0);
        HybridSourceSplit hybridSplit = new HybridSourceSplit(0, mockSplit);
        reader.addSplits(Collections.singletonList(hybridSplit));

        Assert.assertEquals(
                "reader assigned before adding splits", mockSplitReader1, currentReader(reader));
        Assert.assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(readerOutput));

        // drain splits
        InputStatus status = reader.pollNext(readerOutput);
        while (readerOutput.getEmittedRecords().isEmpty() || status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(readerOutput);
            Thread.sleep(10);
        }
        Assert.assertThat(readerOutput.getEmittedRecords(), Matchers.contains(0));
        reader.pollNext(readerOutput);
        Assert.assertEquals(
                "before notifyNoMoreSplits",
                InputStatus.NOTHING_AVAILABLE,
                reader.pollNext(readerOutput));

        Assert.assertThat(readerContext.getSentEvents(), Matchers.emptyIterable());
        reader.notifyNoMoreSplits();
        reader.pollNext(readerOutput);
        Assert.assertThat(readerContext.getSentEvents(), Matchers.iterableWithSize(1));
        Assert.assertEquals(
                0,
                ((SourceReaderFinishedEvent) readerContext.getSentEvents().get(0)).sourceIndex());

        Assert.assertEquals(
                "reader before switch source event", mockSplitReader1, currentReader(reader));
        reader.handleSourceEvents(new SwitchSourceEvent(1, true));
        Assert.assertEquals(
                "reader after switch source event", mockSplitReader2, currentReader(reader));

        reader.notifyNoMoreSplits();
        Assert.assertEquals(
                "reader 1 after notifyNoMoreSplits",
                InputStatus.END_OF_INPUT,
                reader.pollNext(readerOutput));

        reader.close();
    }

    private static SourceReader<Integer, MockSourceSplit> currentReader(
            HybridSourceReader<?> reader) {
        return (SourceReader) Whitebox.getInternalState(reader, "currentReader");
    }
}
