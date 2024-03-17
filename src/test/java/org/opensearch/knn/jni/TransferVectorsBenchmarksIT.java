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

package org.opensearch.knn.jni;

import org.junit.Assert;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TransferVectorsBenchmarksIT extends OpenSearchTestCase {

    private static final Random random = new Random(1212121212);

    private static final int MAX_DATA_SIZE_TO_BE_TRANSFERRED_IN_MB = 100;
    private static final int CONVERSION_CONSTANT = 1024;

    private static final List<Integer> DIMENSIONS_LIST = List.of(1024, 1536,  968, 768, 512, 256, 128);
    //List.of(128, 256, 512, 768, 968, 1024, 1536);

    public void test_transferVectorV2Speed() {
        final List<Double> elapsedTime = new ArrayList<>();
        System.out.println("Detail for Vector transfer based on specific MB: " + MAX_DATA_SIZE_TO_BE_TRANSFERRED_IN_MB);
        System.out.println("Dimension, Elapsed Time");
        for (int i = 0; i < DIMENSIONS_LIST.size(); i++) {
            System.gc();
            int vectorsPerTransfer = (MAX_DATA_SIZE_TO_BE_TRANSFERRED_IN_MB * CONVERSION_CONSTANT * CONVERSION_CONSTANT) / (DIMENSIONS_LIST.get(i) * Float.BYTES);
            elapsedTime.add(transferVectorUtil(DIMENSIONS_LIST.get(i), 1000000, vectorsPerTransfer));
            System.out.println(DIMENSIONS_LIST.get(i) + " , " + elapsedTime.get(i));
        }
        Assert.assertTrue(true);
    }


    public void test_transferVectorOfSpecificCount() {
        final List<Double> elapsedTime = new ArrayList<>();
        int vectorsPerTransfer = 10 * 1000;
        System.out.println("Detail for Vector transfer based on specific size: " + vectorsPerTransfer);
        System.out.println("Dimension, Elapsed Time");
        for (int i = 0; i < DIMENSIONS_LIST.size(); i++) {
            System.gc();
            elapsedTime.add(transferVectorUtil(DIMENSIONS_LIST.get(i), 1000000, vectorsPerTransfer));
            System.out.println(DIMENSIONS_LIST.get(i) + " , " + elapsedTime.get(i));
        }
        Assert.assertTrue(true);
    }

    public void test_transferVectorSendAllAtOnce() {
        final List<Double> elapsedTime = new ArrayList<>();

        Collections.reverse(DIMENSIONS_LIST);

        int vectorsPerTransfer = 1000000;
        System.out.println("Detail for Vector transfer When all vectors sent at once");
        System.out.println("Dimension, Elapsed Time");
        for (int i = 0; i < DIMENSIONS_LIST.size(); i++) {
            System.gc();
            elapsedTime.add(transferVectorUtil(DIMENSIONS_LIST.get(i), 1000000, vectorsPerTransfer));
            System.out.println(DIMENSIONS_LIST.get(i) + " , " + elapsedTime.get(i));
        }
        Assert.assertTrue(true);
    }


    private static float[] generateRandomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = -500 + (float) random.nextGaussian() * (1000);
        }
        return vector;
    }

    private double transferVectorUtil(int dimension, int totalNumberOfVectorsToBeTransferred, int vectorsPerTransfer) {
        List<float[]> vectorList = new ArrayList<>();
        long vectorsAddress = 0;
        long timeInNano = 0;


        for (int i = 0; i < totalNumberOfVectorsToBeTransferred; i++) {
            vectorList.add(generateRandomVector(dimension));
            if (vectorList.size() == vectorsPerTransfer) {
                long startTime = System.nanoTime();
                vectorsAddress = FaissService.transferVectorsV2(vectorsAddress, vectorList.toArray(new float[][]{}));
                long endTime = System.nanoTime();
                timeInNano = timeInNano + (endTime - startTime);
                vectorList = new ArrayList<>();
            }
        }

        if (!CollectionUtils.isEmpty(vectorList)) {
            long startTime = System.nanoTime();
            vectorsAddress = FaissService.transferVectorsV2(vectorsAddress, vectorList.toArray(new float[][]{}));
            long endTime = System.nanoTime();
            timeInNano = timeInNano + (endTime - startTime);
            vectorList = new ArrayList<>();
        }
        FaissService.freeVectors(vectorsAddress);
        return timeInNano / 1000000000d;
    }

}
