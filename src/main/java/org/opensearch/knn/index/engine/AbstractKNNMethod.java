/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.mapper.PerDimensionProcessor;
import org.opensearch.knn.index.mapper.PerDimensionValidator;
import org.opensearch.knn.index.mapper.SpaceVectorValidator;
import org.opensearch.knn.index.mapper.VectorValidator;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;

/**
 * Abstract class for KNN methods. This class provides the common functionality for all KNN methods.
 * It defines the common attributes and methods that all KNN methods should implement.
 */
@AllArgsConstructor
public abstract class AbstractKNNMethod implements KNNMethod {

    protected final MethodComponent methodComponent;
    protected final Set<SpaceType> spaces;

    @Override
    public void resolve(KNNLibraryIndexConfig knnLibraryIndexConfig, KNNLibraryIndex.Builder builder) {
        SpaceType spaceType = knnLibraryIndexConfig.getSpaceType();
        if (!isSpaceTypeSupported(spaceType)) {
            builder.addValidationErrorMessage(
                String.format(
                    Locale.ROOT,
                    "\"%s\" with \"%s\" configuration does not support space type: " + "\"%s\".",
                    this.methodComponent.getName(),
                    knnLibraryIndexConfig.getKnnEngine().getName().toLowerCase(Locale.ROOT),
                    spaceType.getValue()
                )
            );
        }

        // We set these here. If a component during resolution needs to override them, they can. For instance,
        // if we need to use fp16 clip/process functionality, the underlying encoder should override
        builder.vectorValidator(doGetVectorValidator(knnLibraryIndexConfig));
        builder.perDimensionProcessor(doGetPerDimensionProcessor(knnLibraryIndexConfig));
        builder.perDimensionValidator(doGetPerDimensionValidator(knnLibraryIndexConfig));
        builder.quantizationConfig(QuantizationConfig.EMPTY);
        builder.libraryVectorDataType(knnLibraryIndexConfig.getVectorDataType());
        builder.knnLibraryIndexSearchResolver(new DefaultKNNLibraryIndexSearchResolver(knnLibraryIndexConfig));

        Map<String, Object> methodParameters = new HashMap<>();
        methodParameters.put(SPACE_TYPE, spaceType.getValue());
        builder.libraryParameters(methodParameters);
        methodComponent.resolve(knnLibraryIndexConfig.getMethodComponentContext(), builder);
    }

    protected PerDimensionValidator doGetPerDimensionValidator(KNNLibraryIndexConfig knnLibraryIndexConfig) {
        VectorDataType vectorDataType = knnLibraryIndexConfig.getVectorDataType();

        if (VectorDataType.BINARY == vectorDataType) {
            return PerDimensionValidator.DEFAULT_BIT_VALIDATOR;
        }

        if (VectorDataType.BYTE == vectorDataType) {
            return PerDimensionValidator.DEFAULT_BYTE_VALIDATOR;
        }
        return PerDimensionValidator.DEFAULT_FLOAT_VALIDATOR;
    }

    protected VectorValidator doGetVectorValidator(KNNLibraryIndexConfig knnLibraryIndexConfig) {
        SpaceType spaceType = knnLibraryIndexConfig.getSpaceType();
        return new SpaceVectorValidator(spaceType);
    }

    protected PerDimensionProcessor doGetPerDimensionProcessor(KNNLibraryIndexConfig knnLibraryIndexConfig) {
        return PerDimensionProcessor.NOOP_PROCESSOR;
    }

    private boolean isSpaceTypeSupported(SpaceType space) {
        return spaces.contains(space);
    }
}
