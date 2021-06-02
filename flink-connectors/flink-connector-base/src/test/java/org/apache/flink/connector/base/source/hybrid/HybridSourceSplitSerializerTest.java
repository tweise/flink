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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/** Tests for {@link HybridSourceSplitSerializer}. */
public class HybridSourceSplitSerializerTest {

    @Test
    public void testSerialization() throws Exception {
        SimpleVersionedSerializer<SourceSplit> mockSplitSerializer =
                (SimpleVersionedSerializer) new MockSourceSplitSerializer();
        HybridSourceSplitSerializer serializer =
                new HybridSourceSplitSerializer(Collections.singletonList(mockSplitSerializer));
        HybridSourceSplit split = new HybridSourceSplit(0, new MockSourceSplit(1));
        byte[] serialized = serializer.serialize(split);
        HybridSourceSplit clonedSplit = serializer.deserialize(0, serialized);
        Assert.assertEquals(split, clonedSplit);

        try {
            serializer.deserialize(1, serialized);
            Assert.fail();
        } catch (AssertionError e) {
            // expected invalid version
        }
    }
}
