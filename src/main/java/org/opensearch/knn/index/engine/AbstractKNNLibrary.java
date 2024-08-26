/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

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
    public ValidationException resolveKNNIndexContext(KNNIndexContext knnIndexContext, boolean shouldTrain) {
        String methodName = resolveMethod(knnIndexContext);
        throwIllegalArgOnNonNull(validateMethodExists(methodName));
        KNNMethod knnMethod = methods.get(methodName);
        ValidationException validationException = knnMethod.resolveKNNIndexContext(knnIndexContext);
        if (shouldTrain != knnIndexContext.isTrainingRequired()) {
            validationException = ValidationUtil.chainValidationErrors(
                validationException,
                shouldTrain
                    ? "Provided method does not require training, when it should"
                    : "Provided method requires training, but should not."
            );
        }

        validationException = ValidationUtil.chainValidationErrors(validationException, validateDimension(knnIndexContext));
        validationException = ValidationUtil.chainValidationErrors(validationException, validateSpaceType(knnIndexContext));
        return validationException;
    }

    protected String resolveMethod(KNNIndexContext knnIndexContext) {
        KNNMethodContext knnMethodContext = knnIndexContext.getResolvedRequiredParameters().getKnnMethodContext().orElse(null);
        if (knnMethodContext != null && knnMethodContext.getMethodComponentContext().getName().isPresent()) {
            return knnMethodContext.getMethodComponentContext().getName().get();
        }
        return doResolveMethod(knnIndexContext);
    }

    protected abstract String doResolveMethod(KNNIndexContext knnIndexContext);

    private String validateSpaceType(KNNIndexContext knnIndexContext) {
        try {
            knnIndexContext.getSpaceType().validateVectorDataType(knnIndexContext.getVectorDataType());
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
        return null;
    }

    private String validateDimension(KNNIndexContext knnIndexContext) {
        int dimension = knnIndexContext.getDimension();
        KNNEngine knnEngine = knnIndexContext.getKNNEngine();
        if (dimension > KNNEngine.getMaxDimensionByEngine(knnEngine)) {
            return String.format(
                Locale.ROOT,
                "Dimension value cannot be greater than %s for vector with engine: %s",
                KNNEngine.getMaxDimensionByEngine(knnEngine),
                knnEngine.getName()
            );
        }

        if (VectorDataType.BINARY == knnIndexContext.getVectorDataType() && dimension % 8 != 0) {
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

    private void throwIllegalArgOnNonNull(String errorMessage) {
        if (errorMessage != null) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
