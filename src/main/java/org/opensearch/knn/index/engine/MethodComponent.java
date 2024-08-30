/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.Getter;
import org.opensearch.common.TriFunction;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.opensearch.knn.common.KNNConstants.NAME;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;
import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;

/**
 * MethodComponent defines the structure of an individual component that can make up an index
 */
public class MethodComponent {

    @Getter
    private final String name;
    @Getter
    private final Map<String, Parameter<?>> parameters;
    private final BiFunction<MethodComponent, KNNIndexContext, ValidationException> postResolveProcessor;
    private final TriFunction<MethodComponent, MethodComponentContext, KNNIndexContext, Integer> overheadInKBEstimator;
    private final boolean requiresTraining;
    private final Set<VectorDataType> supportedVectorDataTypes;

    /**
     * Constructor
     *
     * @param builder to build method component
     */
    private MethodComponent(Builder builder) {
        this.name = builder.name;
        this.parameters = builder.parameters;
        this.postResolveProcessor = builder.postResolveProcessor;
        this.overheadInKBEstimator = builder.overheadInKBEstimator;
        this.requiresTraining = builder.requiresTraining;
        this.supportedVectorDataTypes = builder.supportedDataTypes;
    }

    public ValidationException postResolveProcess(KNNIndexContext knnIndexContext) {
        if (postResolveProcessor == null) {
            return null;
        }
        return postResolveProcessor.apply(this, knnIndexContext);
    }

    public ValidationException resolveKNNIndexContext(MethodComponentContext methodComponentContext, KNNIndexContext knnIndexContext) {
        ValidationException validationException = null;
        if (!supportedVectorDataTypes.contains(knnIndexContext.getVectorDataType())) {
            validationException = new ValidationException();
            validationException.addValidationError(
                String.format(
                    Locale.ROOT,
                    "Method \"%s\" is not supported for vector data type \"%s\".",
                    name,
                    knnIndexContext.getVectorDataType()
                )
            );
        }

        knnIndexContext.appendTrainingRequirement(requiresTraining);

        /*
        {
            "vector_datatype": "whatever"
            "name": "binary
            "parameters": {
                ...
            }
        }
         */
        Map<String, Object> topLevelParameters = new HashMap<>();
        Map<String, Object> methodParameters = new HashMap<>();
        topLevelParameters.put(NAME, getName());
        topLevelParameters.put(PARAMETERS, methodParameters);
        validationException = ValidationUtil.chainValidationErrors(
            validationException,
            resolveRecursiveParameters(methodComponentContext, knnIndexContext, methodParameters, topLevelParameters)
        );
        knnIndexContext.setLibraryParameters(methodParameters);

        // Next, resolve non-recursive
        validationException = ValidationUtil.chainValidationErrors(
            validationException,
            resolveNonRecursiveParameters(methodComponentContext, knnIndexContext)
        );
        if (knnIndexContext.getLibraryParameters().containsKey(VECTOR_DATA_TYPE_FIELD)) {
            topLevelParameters.put(VECTOR_DATA_TYPE_FIELD, knnIndexContext.getLibraryParameters().get(VECTOR_DATA_TYPE_FIELD));
        }

        knnIndexContext.setLibraryParameters(topLevelParameters);

        // Lastly, increase the estimate
        knnIndexContext.increaseOverheadEstimate(estimateOverheadInKB(methodComponentContext, knnIndexContext));

        return validationException;
    }

    protected ValidationException resolveRecursiveParameters(
        MethodComponentContext methodComponentContext,
        KNNIndexContext knnIndexContext,
        Map<String, Object> methodParameters,
        Map<String, Object> topLevelParameters
    ) {

        ValidationException validationException = null;

        knnIndexContext.setLibraryParameters(methodParameters);
        for (Parameter<?> parameter : parameters.values()) {
            if (parameter instanceof Parameter.MethodComponentContextParameter == false) {
                continue;
            }
            Object innerParameter = extractInnerParameter(parameter.getName(), methodComponentContext);
            validationException = ValidationUtil.chainValidationErrors(
                validationException,
                parameter.resolve(innerParameter, knnIndexContext)
            );
            if (validationException != null) {
                continue;
            }

            if (knnIndexContext.getLibraryParameters().containsKey(VECTOR_DATA_TYPE_FIELD)) {
                topLevelParameters.put(VECTOR_DATA_TYPE_FIELD, knnIndexContext.getLibraryParameters().get(VECTOR_DATA_TYPE_FIELD));
            }

            methodParameters.put(parameter.getName(), knnIndexContext.getLibraryParameters());
        }

        return validationException;
    }

    protected ValidationException resolveNonRecursiveParameters(
        MethodComponentContext methodComponentContext,
        KNNIndexContext knnIndexContext
    ) {
        ValidationException validationException = null;
        for (Parameter<?> parameter : parameters.values()) {
            if (parameter instanceof Parameter.MethodComponentContextParameter) {
                continue;
            }
            Object innerParameter = extractInnerParameter(parameter.getName(), methodComponentContext);
            // In non-recursive case, parameter will not create new map
            validationException = ValidationUtil.chainValidationErrors(
                validationException,
                parameter.resolve(innerParameter, knnIndexContext)
            );
        }

        return validationException;
    }

    private Object extractInnerParameter(String parameter, MethodComponentContext methodComponentContext) {
        if (methodComponentContext == null || methodComponentContext.getParameters().isEmpty() || methodComponentContext.getParameters().get().containsKey(parameter) == false) {
            return null;
        }
        return methodComponentContext.getParameters().get().get(parameter);
    }

    /**
     * Estimates the overhead in KB for this component (without taking into account subcomponents)
     *
     * @param methodComponentContext map of params to estimate overhead for
     * @param knnIndexContext context
     * @return overhead estimate in kb
     */
    public int estimateOverheadInKB(MethodComponentContext methodComponentContext, KNNIndexContext knnIndexContext) {
        if (overheadInKBEstimator == null) {
            return 0;
        }
        return overheadInKBEstimator.apply(this, methodComponentContext, knnIndexContext);
    }

    /**
     * Builder class for MethodComponent
     */
    public static class Builder {

        private final String name;
        private final Map<String, Parameter<?>> parameters;
        private BiFunction<MethodComponent, KNNIndexContext, ValidationException> postResolveProcessor;
        private TriFunction<MethodComponent, MethodComponentContext, KNNIndexContext, Integer> overheadInKBEstimator;
        private boolean requiresTraining;
        private final Set<VectorDataType> supportedDataTypes;

        /**
         * Method to get a Builder instance
         *
         * @param name of method component
         * @return Builder instance
         */
        public static Builder builder(String name) {
            return new Builder(name);
        }

        private Builder(String name) {
            this.name = name;
            this.parameters = new HashMap<>();
            this.supportedDataTypes = new HashSet<>();
        }

        /**
         * Add parameter entry to parameters map
         *
         * @param parameterName name of the parameter
         * @param parameter parameter to be added
         * @return this builder
         */
        public Builder addParameter(String parameterName, Parameter<?> parameter) {
            this.parameters.put(parameterName, parameter);
            return this;
        }

        /**
         * Set the function used to parse a MethodComponentContext as a map
         *
         * @param postResolveProcessor function to parse a MethodComponentContext as a knnLibraryIndexingContext
         * @return this builder
         */
        public Builder setPostResolveProcessor(
            BiFunction<MethodComponent, KNNIndexContext, ValidationException> postResolveProcessor
        ) {
            this.postResolveProcessor = postResolveProcessor;
            return this;
        }

        /**
         * set requiresTraining
         * @param requiresTraining parameter to be set
         * @return Builder instance
         */
        public Builder setRequiresTraining(boolean requiresTraining) {
            this.requiresTraining = requiresTraining;
            return this;
        }

        /**
         * Set the function used to compute an estimate of the size of the component in KB
         *
         * @param overheadInKBEstimator function that will compute the estimation
         * @return Builder instance
         */
        public Builder setOverheadInKBEstimator(
            TriFunction<MethodComponent, MethodComponentContext, KNNIndexContext, Integer> overheadInKBEstimator
        ) {
            this.overheadInKBEstimator = overheadInKBEstimator;
            return this;
        }

        /**
         * Adds supported data types to the method component
         *
         * @param dataTypeSet supported data types
         * @return Builder instance
         */
        public Builder addSupportedDataTypes(Set<VectorDataType> dataTypeSet) {
            supportedDataTypes.addAll(dataTypeSet);
            return this;
        }

        /**
         * Build MethodComponent
         *
         * @return Method Component built from builder
         */
        public MethodComponent build() {
            return new MethodComponent(this);
        }
    }
}
