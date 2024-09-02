/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.Version;
import org.opensearch.common.Explicit;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.engine.KNNEngineResolver;
import org.opensearch.knn.index.engine.KNNLibraryIndex;
import org.opensearch.knn.index.engine.KNNLibraryIndexResolver;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.VectorField;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNLibraryIndexConfig;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.engine.SpaceTypeResolver;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;
import org.opensearch.knn.indices.ModelDao;

import static org.opensearch.knn.common.KNNConstants.KNN_METHOD;
import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;
import static org.opensearch.knn.common.KNNValidationUtil.validateVectorDimension;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.createKNNMethodContextFromLegacy;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.createStoredFieldForByteVector;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.createStoredFieldForFloatVector;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.validateIfCircuitBreakerIsNotTriggered;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.validateIfKNNPluginEnabled;
import static org.opensearch.knn.index.mapper.ModelFieldMapper.UNSET_MODEL_DIMENSION_IDENTIFIER;

/**
 * Field Mapper for KNN vector type. Implementations of this class define what needs to be stored in Lucene's fieldType.
 * This allows us to have alternative mappings for the same field type.
 */
@Log4j2
public abstract class KNNVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "knn_vector";
    public static final String KNN_FIELD = "knn_field";

    private static KNNVectorFieldMapper toType(FieldMapper in) {
        return (KNNVectorFieldMapper) in;
    }

    /**
     * Builder for KNNVectorFieldMapper. This class defines the set of parameters that can be applied to the knn_vector
     * field type
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        protected Boolean ignoreMalformed;

        protected final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        protected final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        protected final Parameter<Integer> dimension = new Parameter<>(
            KNNConstants.DIMENSION,
            false,
            () -> UNSET_MODEL_DIMENSION_IDENTIFIER,
            (n, c, o) -> {
                if (o == null) {
                    throw new IllegalArgumentException("Dimension cannot be null");
                }
                int value;
                try {
                    value = XContentMapValues.nodeIntegerValue(o);
                } catch (Exception exception) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Unable to parse [dimension] from provided value [%s] for vector [%s]", o, name)
                    );
                }
                if (value <= 0) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Dimension value must be greater than 0 for vector: %s", name)
                    );
                }
                return value;
            },
            m -> toType(m).originalParameters.getDimension()
        );

        /**
         * data_type which defines the datatype of the vector values. This is an optional parameter and
         * this is right now only relevant for lucene engine. The default value is float.
         */
        protected final Parameter<VectorDataType> vectorDataType = new Parameter<>(
            VECTOR_DATA_TYPE_FIELD,
            false,
            () -> null,
            (n, c, o) -> VectorDataType.get((String) o),
            m -> toType(m).originalParameters.getVectorDataType()
        );

        /**
         * modelId provides a way for a user to generate the underlying library indices from an already serialized
         * model template index. If this parameter is set, it will take precedence. This parameter is only relevant for
         * library indices that require training.
         */
        protected final Parameter<String> modelId = Parameter.stringParam(
            KNNConstants.MODEL_ID,
            false,
            m -> toType(m).originalParameters.getModelId(),
            null
        );

        protected final Parameter<String> mode = Parameter.restrictedStringParam(
            KNNConstants.MODE_PARAMETER,
            false,
            m -> toType(m).originalParameters.getMode(),
            null,
            WorkloadModeConfig.ON_DISK.getName(),
            WorkloadModeConfig.IN_MEMORY.getName()
        );

        protected final Parameter<String> compressionLevel = Parameter.restrictedStringParam(
            KNNConstants.COMPRESSION_PARAMETER,
            false,
            m -> toType(m).originalParameters.getCompressionLevel(),
            null,
            CompressionConfig.x1.toString(),
            CompressionConfig.x32.toString(),
            CompressionConfig.x16.toString(),
            CompressionConfig.x8.toString()
        );

        /**
         * knnMethodContext parameter allows a user to define their k-NN library index configuration. Defaults to an L2
         * hnsw default engine index without any parameters set
         */
        protected final Parameter<KNNMethodContext> knnMethodContext = new Parameter<>(
            KNN_METHOD,
            false,
            () -> null,
            (n, c, o) -> KNNMethodContext.parse(o),
            m -> toType(m).originalParameters.getKnnMethodContext()
        ).setSerializer(((b, n, v) -> {
            b.startObject(n);
            v.toXContent(b, ToXContent.EMPTY_PARAMS);
            b.endObject();
        }), m -> m.getMethodComponentContext().getName().orElse(null));

        protected final Parameter<Map<String, String>> meta = Parameter.metaParam();

        protected ModelDao modelDao;
        protected Version indexCreatedVersion;

        // This contains the context needed to execute ann searches
        @Setter
        private KNNLibraryIndex knnLibraryIndex;
        @Setter
        private OriginalMappingParameters originalParameters;

        Builder(String name, ModelDao modelDao, Version indexCreatedVersion, OriginalMappingParameters originalParameters) {
            super(name);
            this.modelDao = modelDao;
            this.indexCreatedVersion = indexCreatedVersion;
            this.originalParameters = originalParameters;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(stored, hasDocValues, dimension, vectorDataType, meta, knnMethodContext, modelId, mode, compressionLevel);
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return KNNVectorFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        @Override
        public KNNVectorFieldMapper build(BuilderContext context) {
            validateFullFieldName(context);

            final MultiFields multiFieldsBuilder = this.multiFieldsBuilder.build(this, context);
            final CopyTo copyToBuilder = copyTo.build();
            final Explicit<Boolean> ignoreMalformed = ignoreMalformed(context);
            final Map<String, String> metaValue = meta.getValue();

            if (knnLibraryIndex != null && knnLibraryIndex.getKnnLibraryIndexConfig().getKnnEngine() == KNNEngine.LUCENE) {
                log.debug(String.format(Locale.ROOT, "Use [LuceneFieldMapper] mapper for field [%s]", name));
                LuceneFieldMapper.CreateLuceneFieldMapperInput createLuceneFieldMapperInput = LuceneFieldMapper.CreateLuceneFieldMapperInput
                    .builder()
                    .name(name)
                    .multiFields(multiFieldsBuilder)
                    .copyTo(copyToBuilder)
                    .ignoreMalformed(ignoreMalformed)
                    .stored(stored.getValue())
                    .hasDocValues(hasDocValues.getValue())
                    .originalKnnMethodContext(knnMethodContext.get())
                    .build();
                return LuceneFieldMapper.createFieldMapper(
                    buildFullName(context),
                    metaValue,
                    knnLibraryIndex,
                    originalParameters,
                    createLuceneFieldMapperInput
                );
            }

            if (knnLibraryIndex != null) {
                return MethodFieldMapper.createFieldMapper(
                    buildFullName(context),
                    name,
                    metaValue,
                    multiFieldsBuilder,
                    copyToBuilder,
                    ignoreMalformed,
                    stored.getValue(),
                    hasDocValues.getValue(),
                    knnLibraryIndex,
                    originalParameters

                );
            }

            if (modelId.get() != null) {
                return ModelFieldMapper.createFieldMapper(
                    buildFullName(context),
                    name,
                    metaValue,
                    modelId.get(),
                    multiFieldsBuilder,
                    copyToBuilder,
                    ignoreMalformed,
                    stored.get(),
                    hasDocValues.get(),
                    modelDao,
                    indexCreatedVersion,
                    originalParameters
                );
            }

            return FlatVectorFieldMapper.createFieldMapper(
                buildFullName(context),
                name,
                metaValue,
                dimension.getValue(),
                vectorDataType.get() == null ? VectorDataType.DEFAULT : vectorDataType.get(),
                multiFieldsBuilder,
                copyToBuilder,
                ignoreMalformed,
                stored.get(),
                hasDocValues.get(),
                indexCreatedVersion,
                originalParameters
            );
        }

        /**
         * Validate whether provided full field name contain any invalid characters for physical file name.
         * At the moment, we use a field name as a part of file name while we throw an exception
         * if a physical file name contains any invalid characters when creating snapshot.
         * To prevent from this happening, we restrict vector field name and make sure generated file to have a valid name.
         *
         * @param context : Builder context to have field name info.
         */
        private void validateFullFieldName(final BuilderContext context) {
            final String fullFieldName = buildFullName(context);
            for (char ch : fullFieldName.toCharArray()) {
                if (Strings.INVALID_FILENAME_CHARS.contains(ch)) {
                    throw new MapperParsingException(
                        String.format(
                            Locale.ROOT,
                            "Vector field name must not include invalid characters of %s. "
                                + "Provided field name=[%s] had a disallowed character [%c]",
                            Strings.INVALID_FILENAME_CHARS.stream().map(c -> "'" + c + "'").collect(Collectors.toList()),
                            fullFieldName,
                            ch
                        )
                    );
                }
            }
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        // Use a supplier here because in {@link org.opensearch.knn.KNNPlugin#getMappers()} the ModelDao has not yet
        // been initialized
        private Supplier<ModelDao> modelDaoSupplier;

        public TypeParser(Supplier<ModelDao> modelDaoSupplier) {
            this.modelDaoSupplier = modelDaoSupplier;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new KNNVectorFieldMapper.Builder(name, modelDaoSupplier.get(), parserContext.indexVersionCreated(), null);
            // Parse the parameters. Validation will be done on individual parameters but not taken with context of
            // other parameters
            builder.parse(name, parserContext, node);

            // Validate mix and match on user provided parameters
            BuilderValidator.INSTANCE.validate(builder, isKNNDisabled(parserContext.getSettings()), name);
            OriginalMappingParameters originalParameters = new OriginalMappingParameters(builder);
            builder.setOriginalParameters(originalParameters);

            // Check if we need to get the KNNLibraryIndex and/or further parameters
            if (isKNNDisabled(parserContext.getSettings())) {
                return null;
            }
            if (builder.modelId.get() != null) {
                return null;
            }

            KNNMethodContext resolvedKNNMethodContext = originalParameters.isLegacyMapping()
                ? createKNNMethodContextFromLegacy(parserContext.getSettings(), builder.indexCreatedVersion)
                : builder.knnMethodContext.getValue();
            VectorDataType resolvedVectorDataType = originalParameters.getVectorDataType() == null
                ? VectorDataType.DEFAULT
                : originalParameters.getVectorDataType();
            WorkloadModeConfig resolvedWorkloadModeConfig = WorkloadModeConfig.fromString(originalParameters.getMode());
            CompressionConfig resolvedCompressionConfig = CompressionConfig.fromString(originalParameters.getCompressionLevel());
            KNNLibraryIndexConfig knnLibraryIndexConfig = new KNNLibraryIndexConfig(
                resolvedVectorDataType,
                SpaceTypeResolver.resolveSpaceType(resolvedKNNMethodContext, resolvedVectorDataType),
                KNNEngineResolver.resolveKNNEngine(
                    resolvedKNNMethodContext,
                    resolvedVectorDataType,
                    resolvedWorkloadModeConfig,
                    resolvedCompressionConfig
                ),
                originalParameters.getDimension(),
                Version.CURRENT,
                resolvedKNNMethodContext == null ? MethodComponentContext.EMPTY : resolvedKNNMethodContext.getMethodComponentContext(),
                resolvedWorkloadModeConfig,
                resolvedCompressionConfig,
                false
            );

            // Setup object to track the original parameters provided by the user. We need this to ensure that
            // merging of the field mapper works
            builder.setKnnLibraryIndex(KNNLibraryIndexResolver.resolve(knnLibraryIndexConfig));
            return builder;
        }
    }

    // We store the version of the index with the mapper as different version of Opensearch has different default
    // values of KNN engine Algorithms hyperparameters.
    protected Version indexCreatedVersion;
    protected Explicit<Boolean> ignoreMalformed;
    protected boolean stored;
    protected boolean hasDocValues;
    protected OriginalMappingParameters originalParameters;
    protected ModelDao modelDao;
    protected boolean useLuceneBasedVectorField;

    public KNNVectorFieldMapper(
        String simpleName,
        KNNVectorFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed,
        boolean stored,
        boolean hasDocValues,
        Version indexCreatedVersion,
        OriginalMappingParameters originalParameters
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.stored = stored;
        this.hasDocValues = hasDocValues;
        updateEngineStats();
        this.indexCreatedVersion = indexCreatedVersion;
        this.originalParameters = originalParameters;
    }

    public KNNVectorFieldMapper clone() {
        return (KNNVectorFieldMapper) super.clone();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        parseCreateField(context, fieldType().getDimension(), fieldType().getVectorDataType());
    }

    private Field createVectorField(float[] vectorValue) {
        if (useLuceneBasedVectorField) {
            return new KnnFloatVectorField(name(), vectorValue, fieldType);
        }
        return new VectorField(name(), vectorValue, fieldType);
    }

    private Field createVectorField(byte[] vectorValue) {
        if (useLuceneBasedVectorField) {
            return new KnnByteVectorField(name(), vectorValue, fieldType);
        }
        return new VectorField(name(), vectorValue, fieldType);
    }

    /**
     * Function returns a list of fields to be indexed when the vector is float type.
     *
     * @param array array of floats
     * @return {@link List} of {@link Field}
     */
    protected List<Field> getFieldsForFloatVector(final float[] array) {
        final List<Field> fields = new ArrayList<>();
        fields.add(createVectorField(array));
        if (this.stored) {
            fields.add(createStoredFieldForFloatVector(name(), array));
        }
        return fields;
    }

    /**
     * Function returns a list of fields to be indexed when the vector is byte type.
     *
     * @param array array of bytes
     * @return {@link List} of {@link Field}
     */
    protected List<Field> getFieldsForByteVector(final byte[] array) {
        final List<Field> fields = new ArrayList<>();
        fields.add(createVectorField(array));
        if (this.stored) {
            fields.add(createStoredFieldForByteVector(name(), array));
        }
        return fields;
    }

    /**
     * Validation checks before parsing of doc begins
     */
    protected void validatePreparse() {
        validateIfKNNPluginEnabled();
        validateIfCircuitBreakerIsNotTriggered();
    }

    /**
     * Getter for vector validator after vector parsing
     *
     * @return VectorValidator
     */
    protected abstract VectorValidator getVectorValidator();

    /**
     * Getter for per dimension validator during vector parsing
     *
     * @return PerDimensionValidator
     */
    protected abstract PerDimensionValidator getPerDimensionValidator();

    /**
     * Getter for per dimension processor during vector parsing
     *
     * @return PerDimensionProcessor
     */
    protected abstract PerDimensionProcessor getPerDimensionProcessor();

    protected void parseCreateField(ParseContext context, int dimension, VectorDataType vectorDataType) throws IOException {
        validatePreparse();

        if (VectorDataType.BINARY == vectorDataType || VectorDataType.BYTE == vectorDataType) {
            Optional<byte[]> bytesArrayOptional = getBytesFromContext(context, dimension, vectorDataType);
            if (bytesArrayOptional.isEmpty()) {
                return;
            }
            final byte[] array = bytesArrayOptional.get();
            getVectorValidator().validateVector(array);
            context.doc().addAll(getFieldsForByteVector(array));
        } else if (VectorDataType.FLOAT == vectorDataType) {
            Optional<float[]> floatsArrayOptional = getFloatsFromContext(context, dimension);

            if (floatsArrayOptional.isEmpty()) {
                return;
            }
            final float[] array = floatsArrayOptional.get();
            getVectorValidator().validateVector(array);
            context.doc().addAll(getFieldsForFloatVector(array));
        } else {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Cannot parse context for unsupported values provided for field [%s]", VECTOR_DATA_TYPE_FIELD)
            );
        }

        context.path().remove();
    }

    // Returns an optional array of byte values where each value in the vector is parsed as a float and validated
    // if it is a finite number without any decimals and within the byte range of [-128 to 127].
    Optional<byte[]> getBytesFromContext(ParseContext context, int dimension, VectorDataType dataType) throws IOException {
        context.path().add(simpleName());

        PerDimensionValidator perDimensionValidator = getPerDimensionValidator();
        PerDimensionProcessor perDimensionProcessor = getPerDimensionProcessor();

        ArrayList<Byte> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();

        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                float value = perDimensionProcessor.processByte(context.parser().floatValue());
                perDimensionValidator.validateByte(value);
                vector.add((byte) value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            float value = perDimensionProcessor.processByte(context.parser().floatValue());
            perDimensionValidator.validateByte(value);
            vector.add((byte) value);
            context.parser().nextToken();
        } else if (token == XContentParser.Token.VALUE_NULL) {
            context.path().remove();
            return Optional.empty();
        }
        validateVectorDimension(dimension, vector.size(), dataType);
        byte[] array = new byte[vector.size()];
        int i = 0;
        for (Byte f : vector) {
            array[i++] = f;
        }
        return Optional.of(array);
    }

    Optional<float[]> getFloatsFromContext(ParseContext context, int dimension) throws IOException {
        context.path().add(simpleName());

        PerDimensionValidator perDimensionValidator = getPerDimensionValidator();
        PerDimensionProcessor perDimensionProcessor = getPerDimensionProcessor();

        ArrayList<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();
        float value;
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                value = perDimensionProcessor.process(context.parser().floatValue());
                perDimensionValidator.validate(value);
                vector.add(value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = perDimensionProcessor.process(context.parser().floatValue());
            perDimensionValidator.validate(value);
            vector.add(value);
            context.parser().nextToken();
        } else if (token == XContentParser.Token.VALUE_NULL) {
            context.path().remove();
            return Optional.empty();
        }
        validateVectorDimension(dimension, vector.size(), fieldType().getVectorDataType());

        float[] array = new float[vector.size()];
        int i = 0;
        for (Float f : vector) {
            array[i++] = f;
        }
        return Optional.of(array);
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        Builder mergeBuilder = new KNNVectorFieldMapper.Builder(simpleName(), modelDao, indexCreatedVersion, originalParameters);
        // We cannot get the KNNIndexContext from the model based indices at this field because the
        // cluster state may not be available. So, we need to set it to null.
        if (fieldType().getModelId().isEmpty()) {
            mergeBuilder.setKnnLibraryIndex(fieldType().getKNNLibraryIndex().orElse(null));
        }
        mergeBuilder.init(this);
        BuilderValidator.INSTANCE.validate(mergeBuilder, !fieldType().isIndexedForAnn(), name());
        return mergeBuilder;
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    public KNNVectorFieldType fieldType() {
        return (KNNVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }

    /**
     * Overwrite at child level in case specific stat needs to be updated
     */
    void updateEngineStats() {}

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.putAttribute(KNN_FIELD, "true"); // This attribute helps to determine knn field type
            FIELD_TYPE.freeze();
        }
    }

    private static boolean isKNNDisabled(Settings settings) {
        boolean isSettingPresent = KNNSettings.IS_KNN_INDEX_SETTING.exists(settings);
        return !isSettingPresent || !KNNSettings.IS_KNN_INDEX_SETTING.get(settings);
    }
}
