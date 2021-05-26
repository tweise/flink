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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/** Serializes splits by delegating to the source-indexed underlying split serializer. */
public class HybridSourceSplitSerializer implements SimpleVersionedSerializer<HybridSourceSplit> {

    final List<SimpleVersionedSerializer<SourceSplit>> serializers;

    public HybridSourceSplitSerializer(List<SimpleVersionedSerializer<SourceSplit>> serializers) {
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
            SourceSplit split = serializerOf(sourceIndex).deserialize(nestedVersion, splitBytes);
            return new HybridSourceSplit(sourceIndex, split);
        }
    }

    private SimpleVersionedSerializer<SourceSplit> serializerOf(int sourceIndex) {
        Preconditions.checkArgument(sourceIndex < serializers.size());
        return serializers.get(sourceIndex);
    }
}
