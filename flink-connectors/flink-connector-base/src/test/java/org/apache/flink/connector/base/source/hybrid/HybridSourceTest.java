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
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for {@link HybridSource}. */
public class HybridSourceTest {

    @Test
    public void testBoundedness() {
        HybridSource.SourceChain<Integer, MockSourceSplit, List<MockSourceSplit>> sourceChain;
        sourceChain =
                HybridSource.SourceChain.of(
                        new MockBaseSource(1, 1, Boundedness.BOUNDED),
                        new MockBaseSource(1, 1, Boundedness.BOUNDED));
        assertEquals(Boundedness.BOUNDED, new HybridSource<>(sourceChain).getBoundedness());

        sourceChain =
                HybridSource.SourceChain.of(
                        new MockBaseSource(1, 1, Boundedness.BOUNDED),
                        new MockBaseSource(1, 1, Boundedness.CONTINUOUS_UNBOUNDED));
        assertEquals(
                Boundedness.CONTINUOUS_UNBOUNDED, new HybridSource<>(sourceChain).getBoundedness());

        sourceChain =
                HybridSource.SourceChain.of(
                        new MockBaseSource(1, 1, Boundedness.CONTINUOUS_UNBOUNDED),
                        new MockBaseSource(1, 1, Boundedness.CONTINUOUS_UNBOUNDED));
        try {
            new HybridSource<>(sourceChain);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // boundedness check to fail
        }
    }
}
