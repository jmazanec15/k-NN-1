/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.VectorEncoding;
import org.opensearch.Version;
import org.opensearch.common.Explicit;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNLibraryIndex;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.engine.qframe.QuantizationConfigParser;
import org.opensearch.knn.indices.ModelDao;
import org.opensearch.knn.indices.ModelMetadata;
import org.opensearch.knn.indices.ModelUtil;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.MODEL_ID;
import static org.opensearch.knn.common.KNNConstants.QFRAMEWORK_CONFIG;

/**
 * Field mapper for model in mapping
 */
public class ModelFieldMapper extends KNNVectorFieldMapper {

    // If the dimension has not yet been set because we do not have access to model metadata, it will be -1
    public static final int UNSET_MODEL_DIMENSION_IDENTIFIER = -1;

    private PerDimensionProcessor perDimensionProcessor;
    private PerDimensionValidator perDimensionValidator;
    private VectorValidator vectorValidator;

    public static ModelFieldMapper createFieldMapper(
        String fullname,
        String simpleName,
        Map<String, String> metaValue,
        String modelId,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed,
        boolean stored,
        boolean hasDocValues,
        ModelDao modelDao,
        Version indexCreatedVersion,
        OriginalMappingParameters originalParameters
    ) {
        final KNNVectorFieldType mappedFieldType = new KNNVectorFieldType(fullname, metaValue, () -> {
            ModelMetadata modelMetadata = getModelMetadata(modelDao, modelId);
            KNNLibraryIndex knnLibraryIndex = modelMetadata.getKNNLibraryIndex().orElse(null);
            // This could be better. The issue is that the KNNIndexContext may be null if we dont have
            // access to the method context information.
            return KNNVectorFieldType.KNNVectorFieldTypeConfig.builder()
                .dimension(modelMetadata.getDimension())
                .knnLibraryIndex(knnLibraryIndex)
                .vectorDataType(modelMetadata.getVectorDataType())
                .spaceType(modelMetadata.getSpaceType())
                .knnEngine(modelMetadata.getKnnEngine())
                .build();
        }, modelId);
        return new ModelFieldMapper(
            simpleName,
            mappedFieldType,
            modelId,
            multiFields,
            copyTo,
            ignoreMalformed,
            stored,
            hasDocValues,
            modelDao,
            indexCreatedVersion,
            originalParameters
        );
    }

    private ModelFieldMapper(
        String simpleName,
        KNNVectorFieldType mappedFieldType,
        String modelId,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed,
        boolean stored,
        boolean hasDocValues,
        ModelDao modelDao,
        Version indexCreatedVersion,
        OriginalMappingParameters originalParameters
    ) {
        super(
            simpleName,
            mappedFieldType,
            multiFields,
            copyTo,
            ignoreMalformed,
            stored,
            hasDocValues,
            indexCreatedVersion,
            originalParameters
        );
        this.modelDao = modelDao;

        // For the model field mapper, we cannot validate the model during index creation due to
        // an issue with reading cluster state during mapper creation. So, we need to validate the
        // model when ingestion starts. We do this as lazily as we can
        this.perDimensionProcessor = null;
        this.perDimensionValidator = null;
        this.vectorValidator = null;

        this.fieldType = new FieldType(KNNVectorFieldMapper.Defaults.FIELD_TYPE);
        this.fieldType.putAttribute(MODEL_ID, modelId);
        this.useLuceneBasedVectorField = KNNVectorFieldMapperUtil.useLuceneKNNVectorsFormat(this.indexCreatedVersion);
    }

    @Override
    protected VectorValidator getVectorValidator() {
        initVectorValidator();
        return vectorValidator;
    }

    @Override
    protected PerDimensionValidator getPerDimensionValidator() {
        initPerDimensionValidator();
        return perDimensionValidator;
    }

    @Override
    protected PerDimensionProcessor getPerDimensionProcessor() {
        initPerDimensionProcessor();
        return perDimensionProcessor;
    }

    private void initVectorValidator() {
        if (vectorValidator != null) {
            return;
        }
        vectorValidator = fieldType().getKNNLibraryIndex()
            .map(KNNLibraryIndex::getVectorValidator)
            .orElseGet(() -> new SpaceVectorValidator(fieldType().getSpaceType()));
    }

    private void initPerDimensionValidator() {
        if (perDimensionValidator != null) {
            return;
        }

        perDimensionValidator = fieldType().getKNNLibraryIndex().map(KNNLibraryIndex::getPerDimensionValidator).orElseGet(() -> {
            VectorDataType vectorType = fieldType().getVectorDataType();
            if (vectorType == null) {
                return PerDimensionValidator.DEFAULT_FLOAT_VALIDATOR;
            }
            if (vectorType == VectorDataType.BINARY) {
                return PerDimensionValidator.DEFAULT_BIT_VALIDATOR;
            } else if (vectorType == VectorDataType.BYTE) {
                return PerDimensionValidator.DEFAULT_BYTE_VALIDATOR;
            }
            return PerDimensionValidator.DEFAULT_FLOAT_VALIDATOR;
        });
    }

    private void initPerDimensionProcessor() {
        if (perDimensionProcessor != null) {
            return;
        }
        perDimensionProcessor = fieldType().getKNNLibraryIndex()
            .map(KNNLibraryIndex::getPerDimensionProcessor)
            .orElse(PerDimensionProcessor.NOOP_PROCESSOR);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        validatePreparse();
        KNNLibraryIndex knnIndexContext = fieldType().getKNNLibraryIndex().orElse(null);

        if (useLuceneBasedVectorField && knnIndexContext != null) {
            int adjustedDimension = fieldType().getVectorDataType() == VectorDataType.BINARY
                ? fieldType().getDimension() / Byte.SIZE
                : fieldType().getDimension();
            final VectorEncoding encoding = fieldType().getVectorDataType() == VectorDataType.FLOAT
                ? VectorEncoding.FLOAT32
                : VectorEncoding.BYTE;
            fieldType.setVectorAttributes(
                adjustedDimension,
                encoding,
                knnIndexContext.getSpaceType().getKnnVectorSimilarityFunction().getVectorSimilarityFunction()
            );
        } else {
            fieldType.setDocValuesType(DocValuesType.BINARY);
        }

        // Conditionally add quantization config
        if (knnIndexContext != null) {
            QuantizationConfig quantizationConfig = knnIndexContext.getQuantizationConfig();
            if (quantizationConfig != null && quantizationConfig != QuantizationConfig.EMPTY) {
                this.fieldType.putAttribute(QFRAMEWORK_CONFIG, QuantizationConfigParser.toCsv(quantizationConfig));
            }
        }
        parseCreateField(context, fieldType().getDimension(), fieldType().getVectorDataType());
    }

    private static ModelMetadata getModelMetadata(ModelDao modelDao, String modelId) {
        ModelMetadata modelMetadata = modelDao.getMetadata(modelId);
        if (!ModelUtil.isModelCreated(modelMetadata)) {
            throw new IllegalStateException(String.format("Model ID '%s' is not created.", modelId));
        }
        return modelMetadata;
    }
}
