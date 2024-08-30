/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.engine.validation.ValidationUtil;
import org.opensearch.knn.index.mapper.PerDimensionProcessor;
import org.opensearch.knn.index.mapper.PerDimensionValidator;
import org.opensearch.knn.index.mapper.SpaceVectorValidator;
import org.opensearch.knn.index.mapper.VectorValidator;

import java.util.Locale;
import java.util.Set;

/**
 * Abstract class for KNN methods. This class provides the common functionality for all KNN methods.
 * It defines the common attributes and methods that all KNN methods should implement.
 */
@AllArgsConstructor
public abstract class AbstractKNNMethod implements KNNMethod {

    protected final MethodComponent methodComponent;
    protected final Set<SpaceType> spaces;
    protected final KNNLibrarySearchContext knnLibrarySearchContext;

    @Override
    public ValidationException resolveKNNIndexContext(KNNIndexContext knnIndexContext) {
        ValidationException validationException = null;
        SpaceType spaceType = knnIndexContext.getSpaceType();
        if (!isSpaceTypeSupported(spaceType)) {
            validationException = ValidationUtil.chainValidationErrors(
                validationException,
                String.format(
                    Locale.ROOT,
                    "\"%s\" with \"%s\" configuration does not support space type: " + "\"%s\".",
                    this.methodComponent.getName(),
                    knnIndexContext.getKNNEngine().getName().toLowerCase(Locale.ROOT),
                    spaceType.getValue()
                )
            );
        }

        // We set these here. If a component during resolution needs to override them, they can. For instance,
        // if we need to use fp16 clip/process functionality, the underlying encoder should override
        knnIndexContext.setVectorValidator(doGetVectorValidator(knnIndexContext));
        knnIndexContext.setPerDimensionProcessor(doGetPerDimensionProcessor(knnIndexContext));
        knnIndexContext.setPerDimensionValidator(doGetPerDimensionValidator(knnIndexContext));
        knnIndexContext.setKnnLibrarySearchContext(doGetKNNLibrarySearchContext(knnIndexContext));
        knnIndexContext.setQuantizationConfig(QuantizationConfig.EMPTY);

        MethodComponentContext methodComponentContext = extractUserProvidedMethodComponentContext(knnIndexContext);
        validationException = ValidationUtil.chainValidationErrors(
            validationException,
            methodComponent.resolveKNNIndexContext(methodComponentContext, knnIndexContext)
        );
        if (validationException != null) {
            return validationException;
        }

        if (knnIndexContext.getLibraryParameters().containsKey(KNNConstants.VECTOR_DATA_TYPE_FIELD) == false) {
            knnIndexContext.getLibraryParameters().put(KNNConstants.VECTOR_DATA_TYPE_FIELD, knnIndexContext.getVectorDataType().getValue());
        }

        if (knnIndexContext.getLibraryParameters().containsKey(KNNConstants.SPACE_TYPE) == false) {
            knnIndexContext.getLibraryParameters().put(KNNConstants.SPACE_TYPE, spaceType.getValue());
        }
        return postResolveProcess(knnIndexContext);
    }

    protected ValidationException postResolveProcess(KNNIndexContext knnIndexContext) {
        return methodComponent.postResolveProcess(knnIndexContext);
    }

    protected MethodComponentContext extractUserProvidedMethodComponentContext(KNNIndexContext knnIndexContext) {
        return knnIndexContext.getResolvedRequiredParameters()
            .getKnnMethodContext()
            .map(KNNMethodContext::getMethodComponentContext)
            .orElse(null);
    }

    protected PerDimensionValidator doGetPerDimensionValidator(KNNIndexContext knnIndexContext) {
        VectorDataType vectorDataType = knnIndexContext.getVectorDataType();

        if (VectorDataType.BINARY == vectorDataType) {
            return PerDimensionValidator.DEFAULT_BIT_VALIDATOR;
        }

        if (VectorDataType.BYTE == vectorDataType) {
            return PerDimensionValidator.DEFAULT_BYTE_VALIDATOR;
        }
        return PerDimensionValidator.DEFAULT_FLOAT_VALIDATOR;
    }

    protected VectorValidator doGetVectorValidator(KNNIndexContext knnIndexContext) {
        SpaceType spaceType = knnIndexContext.getSpaceType();
        return new SpaceVectorValidator(spaceType);
    }

    protected PerDimensionProcessor doGetPerDimensionProcessor(KNNIndexContext knnIndexContext) {
        return PerDimensionProcessor.NOOP_PROCESSOR;
    }

    protected KNNLibrarySearchContext doGetKNNLibrarySearchContext(KNNIndexContext knnIndexContext) {
        return knnLibrarySearchContext;
    }

    private boolean isSpaceTypeSupported(SpaceType space) {
        return spaces.contains(space);
    }
}
