/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import java.util.Map;

import org.opensearch.Version;
import org.opensearch.common.Explicit;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.knn.index.engine.KNNMethodConfigContext;

/**
 * Factory class responsible for creating the appropriate {@link KNNVectorFieldMapper}
 * implementation based on the mapping configuration.  The logic that determines
 * which concrete mapper to build was previously embedded in the builder itself
 * which made extending the mapper more difficult.  By centralizing the logic in
 * this factory we make it easier to plug in additional engines or mapper types
 * without modifying the builder.
 */
final class KNNVectorFieldMapperFactory {

    private KNNVectorFieldMapperFactory() {}

    static KNNVectorFieldMapper createFieldMapper(
        KNNVectorFieldMapper.Builder builder,
        Mapper.BuilderContext context,
        Map<String, String> metaValue,
        FieldMapper.MultiFields multiFieldsBuilder,
        FieldMapper.CopyTo copyToBuilder,
        Explicit<Boolean> ignoreMalformed
    ) {

        if (builder.modelId.get() != null) {
            return ModelFieldMapper.createFieldMapper(
                builder.fullFieldName(context),
                builder.name,
                metaValue,
                builder.vectorDataType.getValue(),
                multiFieldsBuilder,
                copyToBuilder,
                ignoreMalformed,
                builder.stored.get(),
                builder.hasDocValues.get(),
                builder.modelDao,
                builder.indexCreatedVersion,
                builder.getOriginalParameters(),
                builder.getKnnMethodConfigContext()
            );
        }

        if (builder.getOriginalParameters().getResolvedKnnMethodContext() == null
            && builder.indexCreatedVersion.onOrAfter(Version.V_2_17_0)) {
            // Ensure docValues are enabled for flat vector fields when required
            builder.adjustDocValuesForFlatMapper();
            return FlatVectorFieldMapper.createFieldMapper(
                builder.fullFieldName(context),
                builder.name,
                metaValue,
                KNNMethodConfigContext.builder()
                    .vectorDataType(builder.vectorDataType.getValue())
                    .versionCreated(builder.indexCreatedVersion)
                    .dimension(builder.dimension.getValue())
                    .build(),
                multiFieldsBuilder,
                copyToBuilder,
                ignoreMalformed,
                builder.stored.get(),
                builder.hasDocValues.get(),
                builder.getOriginalParameters()
            );
        }

        return EngineFieldMapper.createFieldMapper(
            builder.fullFieldName(context),
            builder.name,
            metaValue,
            builder.getKnnMethodConfigContext(),
            multiFieldsBuilder,
            copyToBuilder,
            ignoreMalformed,
            builder.stored.getValue(),
            builder.hasDocValues.get(),
            builder.getOriginalParameters()
        );
    }
}

