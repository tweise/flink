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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitEnumerator;
import org.apache.flink.mock.Whitebox;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link HybridSourceSplitEnumerator}. */
public class HybridSourceSplitEnumeratorTest {

    private static final int SUBTASK0 = 0;
    private static final int SUBTASK1 = 1;

    private MockSplitEnumeratorContext<HybridSourceSplit> context;
    private HybridSourceSplitEnumerator enumerator;
    private HybridSourceSplit splitFromSource0;
    private HybridSourceSplit splitFromSource1;

    private void setupEnumeratorAndTriggerSourceSwitch() {
        context = new MockSplitEnumeratorContext<>(2);
        HybridSource.SourceChain<Integer, List<MockSourceSplit>> sourceChain;
        sourceChain =
                HybridSource.SourceChain.of(
                        new MockBaseSource(1, 1, Boundedness.BOUNDED),
                        new MockBaseSource(1, 1, Boundedness.BOUNDED));
        enumerator = new HybridSourceSplitEnumerator(context, sourceChain, 0);
        enumerator.start();
        // mock enumerator assigns splits once all readers are registered
        registerReader(context, enumerator, SUBTASK0);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.emptyIterable());
        registerReader(context, enumerator, SUBTASK1);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(1));
        splitFromSource0 =
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
        splitFromSource1 =
                context.getSplitsAssignmentSequence().get(1).assignment().get(SUBTASK0).get(0);
        assertEquals(1, splitFromSource1.sourceIndex());
        enumerator.handleSourceEvent(SUBTASK1, new SourceReaderFinishedEvent(SUBTASK1));
        assertEquals("reader without assignment", 1, getCurrentSourceIndex(enumerator));
    }

    @Test
    public void testRegisterReaderAfterSwitchAndReaderReset() {
        setupEnumeratorAndTriggerSourceSwitch();

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
    }

    @Test
    public void testHandleSplitRequestAfterSwitchAndReaderReset() {
        setupEnumeratorAndTriggerSourceSwitch();

        UnderlyingEnumeratorWrapper underlyingEnumeratorWrapper =
                new UnderlyingEnumeratorWrapper(
                        (MockSplitEnumerator)
                                Whitebox.getInternalState(enumerator, "currentEnumerator"));
        Whitebox.setInternalState(enumerator, "currentEnumerator", underlyingEnumeratorWrapper);

        List<MockSourceSplit> mockSourceSplits =
                (List<MockSourceSplit>)
                        Whitebox.getInternalState(underlyingEnumeratorWrapper.enumerator, "splits");
        assertThat(mockSourceSplits, Matchers.emptyIterable());

        // remove reader to suppress auto-assignment behavior of mock source
        context.unregisterReader(SUBTASK0);

        // simulate reader reset to before switch by adding split of previous source back
        context.getSplitsAssignmentSequence().clear();
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource0), SUBTASK0);
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource1), SUBTASK0);
        assertThat(mockSourceSplits, Matchers.contains(splitFromSource1.getWrappedSplit()));
        assertThat(
                "addSplitsBack doesn't trigger assignment when reader not registered",
                context.getSplitsAssignmentSequence(),
                Matchers.emptyIterable());

        enumerator.handleSplitRequest(SUBTASK0, "fakehostname");
        assertSplitAssignment(
                "handleSplitRequest triggers assignment of pending split",
                context,
                1,
                splitFromSource0,
                SUBTASK0);
        assertEquals("current enumerator", 1, getCurrentSourceIndex(enumerator));

        context.getSplitsAssignmentSequence().clear();
        enumerator.handleSplitRequest(SUBTASK0, "fakehostname");
        assertThat(underlyingEnumeratorWrapper.handleSplitRequests, Matchers.emptyIterable());
        assertThat(
                "handleSplitRequest doesn't assign splits",
                context.getSplitsAssignmentSequence(),
                Matchers.emptyIterable());

        // add back the reader so that underlying enumerator assigns splits for second source
        assertThat("pending readers", getPendingReaders(enumerator), Matchers.emptyIterable());
        registerReader(context, enumerator, SUBTASK0);
        assertThat("pending readers", getPendingReaders(enumerator), Matchers.contains(SUBTASK0));
        assertThat(mockSourceSplits, Matchers.contains(splitFromSource1.getWrappedSplit()));
        assertThat(
                "registerReader doesn't trigger split assignment",
                context.getSplitsAssignmentSequence(),
                Matchers.emptyIterable());

        // finish from previous reader triggers switch to next reader
        enumerator.handleSourceEvent(SUBTASK0, new SourceReaderFinishedEvent(SUBTASK0));

        assertThat("pending readers", getPendingReaders(enumerator), Matchers.emptyIterable());
        assertThat(mockSourceSplits, Matchers.emptyIterable());
        assertSplitAssignment(
                "handleSplitRequest triggers assignment of split by underlying enumerator",
                context,
                1,
                splitFromSource1,
                SUBTASK0);

        enumerator.handleSplitRequest(SUBTASK1, "fakehostname");
        assertThat(underlyingEnumeratorWrapper.handleSplitRequests, Matchers.contains(SUBTASK1));
    }

    private static class UnderlyingEnumeratorWrapper
            implements SplitEnumerator<MockSourceSplit, Object> {
        private final List<Integer> handleSplitRequests = new ArrayList<>();
        private final MockSplitEnumerator enumerator;

        private UnderlyingEnumeratorWrapper(MockSplitEnumerator enumerator) {
            this.enumerator = enumerator;
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            handleSplitRequests.add(subtaskId);
            enumerator.handleSplitRequest(subtaskId, requesterHostname);
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addSplitsBack(List splits, int subtaskId) {
            enumerator.addSplitsBack(splits, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            enumerator.addReader(subtaskId);
        }

        @Override
        public Object snapshotState(long checkpointId) throws Exception {
            return enumerator.snapshotState(checkpointId);
        }

        @Override
        public void close() throws IOException {
            enumerator.close();
        }
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

    private static Set<Integer> getPendingReaders(HybridSourceSplitEnumerator enumerator) {
        return (Set<Integer>) Whitebox.getInternalState(enumerator, "pendingReaders");
    }
}
