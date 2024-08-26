/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;

/**
 * Utility class used to resolve the space type of a KNNMethodConfigContext
 */
public class SpaceTypeResolver {
    /**
     * Resolves the engine, given the context
     *
     * @param vectorDataType context to use for resolution
     * @return engine to use for the knn method
     */
    public static SpaceType resolveSpaceType(KNNMethodContext knnMethodContext, VectorDataType vectorDataType) {
        if (knnMethodContext == null) {
            return getDefault(vectorDataType);
        }
        return knnMethodContext.getSpaceType().orElse(getDefault(vectorDataType));
    }

    private static SpaceType getDefault(VectorDataType vectorDataType) {
        if (vectorDataType == VectorDataType.BINARY) {
            return SpaceType.DEFAULT_BINARY;
        }
        return SpaceType.DEFAULT;
    }
}
