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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state. */
public class HybridSourceEnumStateSerializer
        implements SimpleVersionedSerializer<HybridSourceEnumState> {

    private static final int CURRENT_VERSION = 0;

    final List<SimpleVersionedSerializer<Object>> serializers;

    public HybridSourceEnumStateSerializer(List<SimpleVersionedSerializer<Object>> serializers) {
        this.serializers = serializers;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HybridSourceEnumState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(enumState.getCurrentSourceIndex());
            SimpleVersionedSerializer<Object> serializer =
                    serializerOf(enumState.getCurrentSourceIndex());
            out.writeInt(serializer.getVersion());
            byte[] enumStateBytes = serializer.serialize(enumState.getWrappedState());
            out.writeInt(enumStateBytes.length);
            out.write(enumStateBytes);
            return baos.toByteArray();
        }
    }

    @Override
    public HybridSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version != 0) {
            throw new IOException(
                    String.format(
                            "The bytes are serialized with version %d, "
                                    + "while this deserializer only supports version up to %d",
                            version, CURRENT_VERSION));
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int sourceIndex = in.readInt();
            int nestedVersion = in.readInt();
            int length = in.readInt();
            byte[] nestedBytes = new byte[length];
            in.readFully(nestedBytes);
            Object nested = serializerOf(sourceIndex).deserialize(nestedVersion, nestedBytes);
            return new HybridSourceEnumState(sourceIndex, nested);
        }
    }

    private SimpleVersionedSerializer<Object> serializerOf(int sourceIndex) {
        Preconditions.checkArgument(sourceIndex < serializers.size());
        return serializers.get(sourceIndex);
    }
}
