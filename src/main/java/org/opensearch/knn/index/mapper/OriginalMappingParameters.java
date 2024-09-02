/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNMethodContext;

import static org.opensearch.knn.index.mapper.ModelFieldMapper.UNSET_MODEL_DIMENSION_IDENTIFIER;

@Getter
public class OriginalMappingParameters {
    private final VectorDataType vectorDataType;
    private final int dimension;
    private final KNNMethodContext knnMethodContext;
    @Setter
    private KNNMethodContext resolvedKnnMethodContext;
    private final String mode;
    private final String compressionLevel;
    private final String modelId;

    public OriginalMappingParameters(KNNVectorFieldMapper.Builder builder) {
        this.vectorDataType = builder.vectorDataType.get();
        this.knnMethodContext = builder.knnMethodContext.get();
        this.resolvedKnnMethodContext = null;
        this.dimension = builder.dimension.get();
        this.mode = builder.mode.get();
        this.compressionLevel = builder.compressionLevel.get();
        this.modelId = builder.modelId.get();
    }

    public boolean isLegacyMapping() {
        if (knnMethodContext != null) {
            return false;
        }

        if (vectorDataType != VectorDataType.DEFAULT) {
            return false;
        }

        if (modelId != null || dimension == UNSET_MODEL_DIMENSION_IDENTIFIER) {
            return false;
        }

        return mode == null && compressionLevel == null;
    }
}
