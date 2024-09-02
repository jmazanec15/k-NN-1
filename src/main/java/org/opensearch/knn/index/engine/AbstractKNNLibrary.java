/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;

import java.util.Locale;
import java.util.Map;

/**
 * AbstractKNNLibrary implements common functionality shared between libraries
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class AbstractKNNLibrary implements KNNLibrary {
    protected final Map<String, KNNMethod> methods;
    @Getter
    protected final String version;

    @Override
    public KNNLibraryIndex resolve(KNNLibraryIndexConfig knnLibraryIndexConfig) {
        KNNLibraryIndex.Builder builder = KNNLibraryIndex.builder();
        builder.addValidationErrorMessage(
            validateDimension(
                knnLibraryIndexConfig.getDimension(),
                knnLibraryIndexConfig.getVectorDataType(),
                knnLibraryIndexConfig.getKnnEngine()
            )
        );
        builder.addValidationErrorMessage(
            validateSpaceType(knnLibraryIndexConfig.getSpaceType(), knnLibraryIndexConfig.getVectorDataType())
        );
        String methodName = resolveMethod(knnLibraryIndexConfig);
        builder.addValidationErrorMessage(validateMethodExists(methodName), true);
        KNNMethod knnMethod = methods.get(methodName);
        knnMethod.resolve(knnLibraryIndexConfig, builder);
        return builder.build();
    }

    protected String resolveMethod(KNNLibraryIndexConfig resolvedRequiredParameters) {
        MethodComponentContext methodComponentContext = resolvedRequiredParameters.getMethodComponentContext();
        if (methodComponentContext.getName().isPresent()) {
            return methodComponentContext.getName().get();
        }
        return doResolveMethod(resolvedRequiredParameters);
    }

    protected abstract String doResolveMethod(KNNLibraryIndexConfig resolvedRequiredParameters);

    private String validateSpaceType(SpaceType spaceType, VectorDataType vectorDataType) {
        try {
            spaceType.validateVectorDataType(vectorDataType);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
        return null;
    }

    private String validateDimension(int dimension, VectorDataType vectorDataType, KNNEngine knnEngine) {
        int maxDimension = KNNEngine.getMaxDimensionByEngine(knnEngine);
        if (dimension > KNNEngine.getMaxDimensionByEngine(knnEngine)) {
            return String.format(
                Locale.ROOT,
                "Dimension value cannot be greater than %s for vector with library: %s",
                maxDimension,
                knnEngine.getName()
            );
        }

        if (VectorDataType.BINARY == vectorDataType && dimension % 8 != 0) {
            return "Dimension should be multiply of 8 for binary vector data type";
        }

        return null;
    }

    private String validateMethodExists(String methodName) {
        KNNMethod method = methods.get(methodName);
        if (method == null) {
            return String.format(Locale.ROOT, "Invalid method name: %s", methodName);
        }
        return null;
    }
}
