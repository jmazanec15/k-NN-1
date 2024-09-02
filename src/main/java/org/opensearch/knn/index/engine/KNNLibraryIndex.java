/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.Version;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.mapper.PerDimensionProcessor;
import org.opensearch.knn.index.mapper.PerDimensionValidator;
import org.opensearch.knn.index.mapper.VectorValidator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class provides all of the configuration information needed to build {@link KNNLibrary} indices, and also search
 * them
 */
@Getter
@AllArgsConstructor
@Builder(builderClassName = "Builder")
public final class KNNLibraryIndex {
    // Potentially recursive
    private final Map<String, Object> libraryParameters;
    private final KNNLibraryIndexSearchResolver knnLibraryIndexSearchResolver;
    private final QuantizationConfig quantizationConfig;
    // Type after quantization is applied
    private final VectorDataType libraryVectorDataType;

    private final VectorValidator vectorValidator;
    private final PerDimensionValidator perDimensionValidator;
    private final PerDimensionProcessor perDimensionProcessor;
    private int estimatedIndexOverhead;

    // non-configurable
    private final KNNLibraryIndexConfig knnLibraryIndexConfig;

    public static class Builder {
        @Getter
        private final Set<String> validationMessages;

        public Builder() {
            this.validationMessages = new HashSet<>();
        }

        public KNNLibraryIndexSearchResolver getKnnLibraryIndexSearchResolver() {
            return knnLibraryIndexSearchResolver;
        }

        public PerDimensionProcessor getPerDimensionProcessor() {
            return perDimensionProcessor;
        }

        public PerDimensionValidator getPerDimensionValidator() {
            return perDimensionValidator;
        }

        public VectorDataType getLibraryVectorDataType() {
            return libraryVectorDataType;
        }

        public Map<String, Object> getLibraryParameters() {
            return libraryParameters;
        }

        public KNNLibraryIndexConfig getKnnLibraryIndexConfig() {
            return knnLibraryIndexConfig;
        }

        public void incEstimatedIndexOverhead(int estimatedIndexOverhead) {
            this.estimatedIndexOverhead += estimatedIndexOverhead;
        }

        public Builder addValidationErrorMessage(String errorMessage, boolean shouldThrowOnInvalid) {
            if (errorMessage == null) {
                return this;
            }
            validationMessages.add(errorMessage);
            if (shouldThrowOnInvalid) {
                throwIfInvalid();
            }
            return this;
        }

        public Builder addValidationErrorMessage(String errorMessage) {
            return addValidationErrorMessage(errorMessage, false);
        }

        public Builder addValidationErrorMessages(Set<String> errorMessages, boolean shouldThrowOnInvalid) {
            if (errorMessages == null) {
                return this;
            }

            for (String errorMessage : errorMessages) {
                addValidationErrorMessage(errorMessage);
            }

            if (shouldThrowOnInvalid) {
                throwIfInvalid();
            }

            return this;
        }

        public Builder addValidationErrorMessages(Set<String> errorMessages) {
            return addValidationErrorMessages(errorMessages, false);
        }

        public KNNLibraryIndex build() {
            throwIfInvalid();
            return new KNNLibraryIndex(
                libraryParameters,
                knnLibraryIndexSearchResolver,
                quantizationConfig,
                libraryVectorDataType,
                vectorValidator,
                perDimensionValidator,
                perDimensionProcessor,
                estimatedIndexOverhead,
                knnLibraryIndexConfig
            );
        }

        private void throwIfInvalid() {
            if (validationMessages.isEmpty() == false) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationErrors(validationMessages);
                throw validationException;
            }
        }
    }

    // NIce to have getters
    public SpaceType getSpaceType() {
        return knnLibraryIndexConfig.getSpaceType();
    }

    public int getDimension() {
        return knnLibraryIndexConfig.getDimension();
    }

    public VectorDataType getVectorDataType() {
        return knnLibraryIndexConfig.getVectorDataType();
    }

    public Version getCreatedVersion() {
        return knnLibraryIndexConfig.getCreatedVersion();
    }
}
