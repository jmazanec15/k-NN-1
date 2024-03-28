/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn;

import lombok.SneakyThrows;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.knn.index.codec.util.KNNVectorSerializer;
import org.opensearch.knn.index.codec.util.KNNVectorSerializerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3)
@Measurement(iterations = 4)
@Fork(value = 1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Timeout(time = 4, timeUnit = TimeUnit.HOURS)
public class VectorSerdeBenchmarks {
    private static final Random random = new Random(1212121212);
    private static final int NUM_VECS = 500000;

    @Param({ "128", "512" })
    private int dimension;

    @Param({ "Lucene", "Plugin" })
    private String mode;

    private float[][] vectorList;

    @Setup(Level.Trial)
    public void setup() {
        System.out.println("Setting up");
        vectorList = new float[NUM_VECS][dimension];
        for (int i = 0; i < NUM_VECS; i++) {
            vectorList[i] = generateRandomVector(dimension);
        }
    }

    @Benchmark
    public void serde() throws IOException {
        List<byte[]> byteArrays = serialize();
        assert byteArrays.size() == NUM_VECS;
//        float[][] floats = deserialize(byteArrays);
//        assert floats.length == NUM_VECS;
    }

    private List<byte[]> serialize() {
        List<byte[]> byteArrays = new ArrayList<>();
        if (Objects.equals(mode, "Lucene")) {
            System.out.println("Lucene serialize");
            // Inspired by
            // https://github.com/apache/lucene/blob/ad8545151de8a2e32153b9b254fc7743aeaa6393/lucene/core/src/java/org/apache/lucene/codecs/lucene99/Lucene99FlatVectorsWriter.java#L166-L174
            for (Object v : vectorList) {
                final ByteBuffer buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                buffer.asFloatBuffer().put((float[]) v);
                byteArrays.add(buffer.array());
            }
        } else {
            System.out.println("Plugin serialize");
            // Inspired by
            // https://github.com/opensearch-project/k-NN/blob/c861966708219b5a0c27fa60e6eb1c150dfc0efa/src/main/java/org/opensearch/knn/index/VectorField.java#L20
            for (int vec = 0; vec < NUM_VECS; vec++) {
                final KNNVectorSerializer vectorSerializer = KNNVectorSerializerFactory.getDefaultSerializer();
                byteArrays.add(vectorSerializer.floatToByteArray(vectorList[vec]));
            }
        }
        assert byteArrays.size() == NUM_VECS;
        return byteArrays;
    }

    private float[][] deserialize(List<byte[]> bytes) throws IOException {
        float[][] floats = new float[NUM_VECS][];
        if (Objects.equals(mode, "Lucene")) {
            // Inspired by
            // https://github.com/apache/lucene/blob/ad8545151de8a2e32153b9b254fc7743aeaa6393/lucene/core/src/java/org/apache/lucene/codecs/lucene95/OffHeapFloatVectorValues.java#L60-L67
            System.out.println("Lucene deserialize");
            for (int i = 0; i < NUM_VECS; i++) {
                try (IndexInput indexInput = new ByteArrayIndexInput("blah", bytes.get(i))){
                    float[] vec = new float[dimension];
                    indexInput.seek(0);
                    indexInput.readFloats(vec, 0, dimension);
                    floats[i] = vec;
                }
            }
        } else {
            // Inspired by
            // https://github.com/opensearch-project/k-NN/blob/2.13/src/main/java/org/opensearch/knn/index/codec/util/KNNCodecUtil.java#L45-L53
            // and
            // https://github.com/opensearch-project/k-NN/blob/c861966708219b5a0c27fa60e6eb1c150dfc0efa/src/main/java/org/opensearch/knn/index/KNNVectorScriptDocValues.java#L46
            System.out.println("Plugin deserialize");
            for (int i = 0; i < NUM_VECS; i++) {
                try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes.get(i), 0, bytes.size())) {
                    final KNNVectorSerializer vectorSerializer = KNNVectorSerializerFactory.getDefaultSerializer();
                    floats[i] = vectorSerializer.byteToFloatArray(byteStream);
                }
            }
        }
        return floats;
    }

    private float[] generateRandomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = -500 + (float) random.nextGaussian() * (1000);
        }
        return vector;
    }
}
