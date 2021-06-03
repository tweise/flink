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
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.mock.Whitebox;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link HybridSourceSplitEnumerator}. */
public class HybridSourceSplitEnumeratorTest {

    private static final int SUBTASK0 = 0;
    private static final int SUBTASK1 = 1;

    @Test
    public void testSwitchSource() {
        MockSplitEnumeratorContext<HybridSourceSplit> context = new MockSplitEnumeratorContext<>(2);
        HybridSource.SourceChain<Integer, List<MockSourceSplit>> sourceChain;
        sourceChain =
                HybridSource.SourceChain.of(
                        new MockBaseSource(1, 1, Boundedness.BOUNDED),
                        new MockBaseSource(1, 1, Boundedness.BOUNDED));
        HybridSourceSplitEnumerator enumerator =
                new HybridSourceSplitEnumerator(context, sourceChain, 0);
        enumerator.start();
        // mock enumerator assigns splits once all readers are registered
        registerReader(context, enumerator, SUBTASK0);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.emptyIterable());
        registerReader(context, enumerator, SUBTASK1);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(1));
        HybridSourceSplit splitFromSource0 =
                context.getSplitsAssignmentSequence().get(0).assignment().get(SUBTASK0).get(0);
        assertEquals(0, splitFromSource0.sourceIndex());
        assertEquals(0, getCurrentSourceIndex(enumerator));

        // trigger source switch
        enumerator.handleSourceEvent(SUBTASK0, new SourceReaderFinishedEvent(SUBTASK0));
        assertEquals("all assignments finished", 1, getCurrentSourceIndex(enumerator));
        assertThat(
                "switch triggers split assignment",
                context.getSplitsAssignmentSequence(),
                Matchers.iterableWithSize(3));
        HybridSourceSplit splitFromSource1 =
                context.getSplitsAssignmentSequence().get(1).assignment().get(SUBTASK0).get(0);
        assertEquals(1, splitFromSource1.sourceIndex());
        enumerator.handleSourceEvent(SUBTASK1, new SourceReaderFinishedEvent(SUBTASK1));
        assertEquals("reader without assignment", 1, getCurrentSourceIndex(enumerator));

        // add split of previous source back (simulates reader reset during recovery)
        context.getSplitsAssignmentSequence().clear();
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource0), SUBTASK0);
        assertSplitAssignment(
                "addSplitsBack triggers assignment when reader registered",
                context,
                1,
                splitFromSource0,
                SUBTASK0);

        // remove reader from context
        context.getSplitsAssignmentSequence().clear();
        context.unregisterReader(SUBTASK0);
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource0), SUBTASK0);
        assertThat(
                "addSplitsBack doesn't trigger assignment when reader not registered",
                context.getSplitsAssignmentSequence(),
                Matchers.emptyIterable());
        registerReader(context, enumerator, SUBTASK0);
        assertSplitAssignment(
                "registerReader triggers assignment", context, 1, splitFromSource0, SUBTASK0);

        // enumerator.handleSplitRequest(SUBTASK0, "fakehostname");
        // enumerator.handleSplitRequest(SUBTASK1, "fakehostname");
    }

    private static void assertSplitAssignment(
            String reason,
            MockSplitEnumeratorContext<HybridSourceSplit> context,
            int size,
            HybridSourceSplit split,
            int subtask) {
        assertThat(reason, context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(size));
        assertEquals(
                reason,
                split,
                context.getSplitsAssignmentSequence()
                        .get(size - 1)
                        .assignment()
                        .get(subtask)
                        .get(0));
    }

    private static void registerReader(
            MockSplitEnumeratorContext<HybridSourceSplit> context,
            HybridSourceSplitEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location 0"));
        enumerator.addReader(reader);
    }

    private static int getCurrentSourceIndex(HybridSourceSplitEnumerator enumerator) {
        return (int) Whitebox.getInternalState(enumerator, "currentSourceIndex");
    }
}
