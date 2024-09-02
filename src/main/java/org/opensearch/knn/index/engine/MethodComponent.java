/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.Getter;
import org.opensearch.knn.index.VectorDataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.opensearch.knn.common.KNNConstants.PARAMETERS;

/**
 * MethodComponent defines the structure of an individual component that can make up an index
 */
public class MethodComponent {

    @Getter
    private final String name;
    @Getter
    private final Map<String, Parameter<?>> parameters;
    private final BiConsumer<MethodComponent, KNNLibraryIndex.Builder> postResolveProcessor;
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
        this.requiresTraining = builder.requiresTraining;
        this.supportedVectorDataTypes = builder.supportedDataTypes;
    }

    /**
     * Resolve KNNLibraryIndex.Builder for the provide {@link KNNLibraryIndexConfig} and {@link MethodComponentContext}.
     * In general, a {@link MethodComponent} is an individual component of an overall k-NN index.
     *
     * @param methodComponentContext {@link MethodComponentContext}
     * @param builder {@link KNNLibraryIndex.Builder}
     */
    public void resolve(MethodComponentContext methodComponentContext, KNNLibraryIndex.Builder builder) {
        if (!supportedVectorDataTypes.contains(builder.getKnnLibraryIndexConfig().getVectorDataType())) {
            builder.addValidationErrorMessage(
                String.format(
                    Locale.ROOT,
                    "Method \"%s\" is not supported for vector data type \"%s\".",
                    name,
                    builder.getKnnLibraryIndexConfig().getVectorDataType()
                ),
                true
            );
        }

        if (builder.getKnnLibraryIndexConfig().isShouldIndexConfigRequireTraining() != requiresTraining) {
            builder.addValidationErrorMessage("Make this a better message!");
        }

        Map<String, Object> libraryParameters = builder.getLibraryParameters();
        Map<String, Object> subParametersMap = new HashMap<>();

        libraryParameters.put(PARAMETERS, subParametersMap);

        builder.libraryParameters(subParametersMap);
        resolveNonRecursiveParameters(builder, methodComponentContext);
        resolveRecursiveParameters(builder, methodComponentContext);
        builder.libraryParameters(libraryParameters);
        postResolveProcess(builder);
    }

    protected void resolveNonRecursiveParameters(KNNLibraryIndex.Builder builder, MethodComponentContext methodComponentContext) {
        for (Parameter<?> parameter : parameters.values()) {
            if (parameter instanceof Parameter.MethodComponentContextParameter) {
                continue;
            }
            Object innerParameter = extractInnerParameter(parameter.getName(), methodComponentContext);
            parameter.resolve(innerParameter, builder);
        }
    }

    protected void resolveRecursiveParameters(KNNLibraryIndex.Builder builder, MethodComponentContext methodComponentContext) {
        for (Parameter<?> parameter : parameters.values()) {
            if (parameter instanceof Parameter.MethodComponentContextParameter == false) {
                continue;
            }

            Object innerParameter = extractInnerParameter(parameter.getName(), methodComponentContext);
            Map<String, Object> parametersMap = builder.getLibraryParameters();
            Map<String, Object> subParametersMap = new HashMap<>();
            parametersMap.put(parameter.getName(), subParametersMap);
            builder.libraryParameters(subParametersMap);
            parameter.resolve(innerParameter, builder);
            builder.libraryParameters(parametersMap);
        }
    }

    protected void postResolveProcess(KNNLibraryIndex.Builder builder) {
        if (postResolveProcessor != null) {
            postResolveProcessor.accept(this, builder);
        }
    }

    private Object extractInnerParameter(String parameter, MethodComponentContext methodComponentContext) {
        if (methodComponentContext == null
            || methodComponentContext.getParameters().isEmpty()
            || methodComponentContext.getParameters().get().containsKey(parameter) == false) {
            return null;
        }
        return methodComponentContext.getParameters().get().get(parameter);
    }

    /**
     * Builder class for MethodComponent
     */
    public static class Builder {

        private final String name;
        private final Map<String, Parameter<?>> parameters;
        private BiConsumer<MethodComponent, KNNLibraryIndex.Builder> postResolveProcessor;
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
        public Builder setPostResolveProcessor(BiConsumer<MethodComponent, KNNLibraryIndex.Builder> postResolveProcessor) {
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
