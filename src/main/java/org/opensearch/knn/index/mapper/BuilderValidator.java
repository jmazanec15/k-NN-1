/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import org.opensearch.index.mapper.MapperParsingException;

import java.util.Locale;

import static org.opensearch.knn.index.mapper.ModelFieldMapper.UNSET_MODEL_DIMENSION_IDENTIFIER;

// Helper class used to validate builder before build is called. Needs to be invoked in 2 places: during
// parsing and during merge.
final class BuilderValidator {

    final static BuilderValidator INSTANCE = new BuilderValidator();

    void validate(KNNVectorFieldMapper.Builder builder, boolean isKNNDisabled, String name) {
        if (isKNNDisabled) {
            validateFromFlat(builder, name);
        } else if (builder.modelId.get() != null) {
            validateFromModel(builder, name);
        } else {
            validateFromKNNMethod(builder, name);
        }
    }

    private void validateFromFlat(KNNVectorFieldMapper.Builder builder, String name) {
        if (builder.modelId.get() != null || builder.knnMethodContext.get() != null) {
            throw new MapperParsingException("Cannot set modelId or method parameters when index.knn setting is false for field: %s");
        }
        validateDimensionSet(builder, "flat");
        validateCompressionAndModeNotSet(builder, name, "flat");
    }

    private void validateFromModel(KNNVectorFieldMapper.Builder builder, String name) {
        // Dimension should not be null unless modelId is used
        if (builder.dimension.getValue() != UNSET_MODEL_DIMENSION_IDENTIFIER) {
            throw new MapperParsingException(
                String.format(Locale.ROOT, "Dimension cannot be specified for model index for field: %s", builder.name())
            );
        }
        validateMethodAndModelNotBothSet(builder, name);
        validateCompressionAndModeNotSet(builder, name, "model");
        validateVectorDataTypeNotSet(builder, name, "model");
    }

    private void validateFromKNNMethod(KNNVectorFieldMapper.Builder builder, String name) {
        validateMethodAndModelNotBothSet(builder, name);
        validateDimensionSet(builder, "method");
    }

    private void validateVectorDataTypeNotSet(KNNVectorFieldMapper.Builder builder, String name, String context) {
        if (builder.vectorDataType.isConfigured()) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "Vector data type can not be specified in a %s mapping configuration for field: %s",
                    context,
                    name
                )
            );
        }
    }

    private void validateCompressionAndModeNotSet(KNNVectorFieldMapper.Builder builder, String name, String context) {
        if (builder.mode.isConfigured() == true || builder.compressionLevel.isConfigured() == true) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "Compression and mode can not be specified in a %s mapping configuration for field: %s",
                    context,
                    name
                )
            );
        }
    }

    private void validateMethodAndModelNotBothSet(KNNVectorFieldMapper.Builder builder, String name) {
        if (builder.knnMethodContext.isConfigured() == true && builder.modelId.isConfigured() == true) {
            throw new MapperParsingException(
                String.format(Locale.ROOT, "Method and model can not be both specified in the mapping: %s", name)
            );
        }
    }

    private void validateDimensionSet(KNNVectorFieldMapper.Builder builder, String context) {
        if (builder.dimension.getValue() == UNSET_MODEL_DIMENSION_IDENTIFIER) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "Dimension value must be set in a %s mapping configuration for field: %s",
                    context,
                    builder.name()
                )
            );
        }
    }
}
