/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.util;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.IntStream;

/**
 * Class implements KNNVectorSerializer based on serialization/deserialization of array as collection of individual numbers
 */
public class KNNVectorAsCollectionOfFloatsSerializer implements KNNVectorSerializer {
    private static final int BYTES_IN_FLOAT = 4;

    @Override
    public byte[] floatToByteArray(float[] input) {
        final ByteBuffer bb = ByteBuffer.allocate(input.length * BYTES_IN_FLOAT).order(ByteOrder.BIG_ENDIAN);
        IntStream.range(0, input.length).forEach((index) -> bb.putFloat(input[index]));
        byte[] bytes = new byte[bb.flip().limit()];
        bb.get(bytes);
        return bytes;
    }

    @Override
    public float[] byteToFloatArray(BytesRef bytesRef) {
        if (bytesRef == null || (bytesRef.length - bytesRef.offset) % BYTES_IN_FLOAT != 0) {
            throw new IllegalArgumentException("Byte stream cannot be deserialized to array of floats");
        }
        final int sizeOfFloatArray = (bytesRef.length - bytesRef.offset) / BYTES_IN_FLOAT;
        final float[] vector = new float[sizeOfFloatArray];
        ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).asFloatBuffer().get(vector);
        return vector;
    }

    @Override
    public void byteToFloatArray(BytesRef bytesRef, float[] dest) {
        if (bytesRef == null || (bytesRef.length - bytesRef.offset) % BYTES_IN_FLOAT != 0) {
            throw new IllegalArgumentException("Byte stream cannot be deserialized to array of floats");
        }
        readFloats(dest, bytesRef);
    }

    private void readFloats(float[] floats, BytesRef bytesRef) {
        int offset2 = 0;
        for (int i = 0; i < floats.length; i++) {
            floats[i] = Float.intBitsToFloat(readInt(bytesRef, offset2));
            offset2 += 4;
        }
    }

    private int readInt(BytesRef bytesRef, int offset) {
        final byte b1 = bytesRef.bytes[bytesRef.offset + offset];
        final byte b2 = bytesRef.bytes[bytesRef.offset + offset + 1];
        final byte b3 = bytesRef.bytes[bytesRef.offset + offset + 2];
        final byte b4 = bytesRef.bytes[bytesRef.offset + offset + 3];
        // return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
        return ((b1 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 8) | (b4 & 0xFF);
    }
}
