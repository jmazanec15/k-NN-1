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

import com.google.common.collect.ImmutableMap;
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
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.knn.jni.JNIService;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.opensearch.knn.common.KNNConstants.FAISS_NAME;
import static org.opensearch.knn.common.KNNConstants.NMSLIB_NAME;
import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;

@Warmup(iterations = 2)
@Measurement(iterations = 4)
@Fork(value = 1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Timeout(time = 4, timeUnit = TimeUnit.HOURS)
public class BuildNativeIndexBenchmarks {
    private static final Random random = new Random(1212121212);
    private static final int NUM_VECS = 250000;

    @Param({ "128", "512" })
    private int dimension;

    @Param({"innerproduct", "l2" })
    private String spaceType;

    @Param({ "16" })
    private int m;
    @Param({ "100" })
    private int efConstruction;
    @Param({ "4" })
    private int indexThreadQty;
    //@Param({NMSLIB_NAME, FAISS_NAME})
    @Param({FAISS_NAME})
    private String engine;

    private float[][] vectorList;
    private int[] ids;

    @Setup(Level.Trial)
    public void setup() {
        System.out.println("Setting up");
        ids = fromRange(1, NUM_VECS);
        vectorList = new float[NUM_VECS][dimension];
        for (int i = 0; i < NUM_VECS; i++) {
            vectorList[i] = generateRandomVector(dimension);
        }
    }

    @Benchmark
    public void buildNativeIndex() {
        System.out.println("LD_PRELOAD: " + System.getenv("LD_PRELOAD"));
        if (Objects.equals(engine, NMSLIB_NAME)) {
            buildNmslib();
        } else {
            buildFaiss();
        }
    }

    public void buildFaiss() {
        System.out.println("Building with faiss");
        JNIService.createIndex(
                ids,
                vectorList,
                createIndexName(),
                ImmutableMap.of(
                        SPACE_TYPE, spaceType,
                        KNNConstants.PARAMETERS, ImmutableMap.of(KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction),
                        KNNConstants.INDEX_DESCRIPTION_PARAMETER, String.format("HNSW%d,Flat", m),
                        KNNConstants.INDEX_THREAD_QTY, indexThreadQty
                ),
                KNNEngine.FAISS
        );
    }

    public void buildNmslib() {
        System.out.println("Building with nmslib");
        JNIService.createIndex(
                ids,
                vectorList,
                createIndexName(),
                ImmutableMap.of(
                        SPACE_TYPE, spaceType,
                        KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction,
                        KNNConstants.METHOD_PARAMETER_M, m,
                        KNNConstants.INDEX_THREAD_QTY, indexThreadQty
                ),
                KNNEngine.NMSLIB
        );
    }

    private String createIndexName() {
        return "/dev/null";
    }

    private int[] fromRange(int startInclusive, int endInclusive) {
        return IntStream.rangeClosed(startInclusive, endInclusive).toArray();
    }

    private float[] generateRandomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = -500 + (float) random.nextGaussian() * (1000);
        }
        return vector;
    }
}
