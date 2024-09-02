/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;

import static org.opensearch.knn.index.engine.KNNEngine.FAISS;
import static org.opensearch.knn.index.engine.KNNEngine.NMSLIB;

/**
 * Utility class used to resolve the engine for a k-NN method config context
 */
public class KNNEngineResolver {

    /**
     * Resolves the engine, given the context
     *
     * @param knnMethodContext user provided context
     * @param vectorDataType data type of the vector field
     * @param workloadModeConfig workload mode config to use for the knn method
     * @param compressionConfig compression config to use for the knn method
     * @return engine to use for the knn method
     */
    public static KNNEngine resolveKNNEngine(
        KNNMethodContext knnMethodContext,
        VectorDataType vectorDataType,
        WorkloadModeConfig workloadModeConfig,
        CompressionConfig compressionConfig
    ) {
        if (knnMethodContext == null) {
            return getDefault(vectorDataType, workloadModeConfig, compressionConfig);
        }

        return knnMethodContext.getKnnEngine().orElse(getDefault(vectorDataType, workloadModeConfig, compressionConfig));
    }

    private static KNNEngine getDefault(
        VectorDataType vectorDataType,
        WorkloadModeConfig workloadModeConfig,
        CompressionConfig compressionConfig
    ) {
        // Need to use FAISS by default if not using float type
        if (vectorDataType != VectorDataType.FLOAT) {
            return FAISS;
        }

        // If the user has set compression or workload we need to return faiss
        if (isWorkloadSet(workloadModeConfig) || isCompressionSet(compressionConfig)) {
            return FAISS;
        }

        return NMSLIB;
    }

    private static boolean isWorkloadSet(WorkloadModeConfig workloadModeConfig) {
        return workloadModeConfig != WorkloadModeConfig.NOT_CONFIGURED && workloadModeConfig != WorkloadModeConfig.DEFAULT;
    }

    private static boolean isCompressionSet(CompressionConfig compressionConfig) {
        return compressionConfig != CompressionConfig.NOT_CONFIGURED && compressionConfig != CompressionConfig.DEFAULT;
    }
}
