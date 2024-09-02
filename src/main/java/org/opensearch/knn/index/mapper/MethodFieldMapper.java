/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.VectorEncoding;
import org.opensearch.common.Explicit;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNLibraryIndex;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.engine.qframe.QuantizationConfigParser;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;
import static org.opensearch.knn.common.KNNConstants.QFRAMEWORK_CONFIG;
import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;

/**
 * Field mapper for method definition in mapping
 */
public class MethodFieldMapper extends KNNVectorFieldMapper {

    private final PerDimensionProcessor perDimensionProcessor;
    private final PerDimensionValidator perDimensionValidator;
    private final VectorValidator vectorValidator;

    public static MethodFieldMapper createFieldMapper(
        String fullname,
        String simpleName,
        Map<String, String> metaValue,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed,
        boolean stored,
        boolean hasDocValues,
        KNNLibraryIndex knnLibraryIndex,
        OriginalMappingParameters originalParameters
    ) {
        final KNNVectorFieldType mappedFieldType = new KNNVectorFieldType(
            fullname,
            metaValue,
            () -> KNNVectorFieldType.KNNVectorFieldTypeConfig.builder()
                .dimension(knnLibraryIndex.getDimension())
                .knnLibraryIndex(knnLibraryIndex)
                .vectorDataType(knnLibraryIndex.getVectorDataType())
                .spaceType(knnLibraryIndex.getSpaceType())
                .knnEngine(knnLibraryIndex.getKnnLibraryIndexConfig().getKnnEngine())
                .build(),
            null
        );
        return new MethodFieldMapper(
            simpleName,
            mappedFieldType,
            multiFields,
            copyTo,
            ignoreMalformed,
            stored,
            hasDocValues,
            knnLibraryIndex,
            originalParameters
        );
    }

    private MethodFieldMapper(
        String simpleName,
        KNNVectorFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed,
        boolean stored,
        boolean hasDocValues,
        KNNLibraryIndex knnLibraryIndex,
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
            knnLibraryIndex.getCreatedVersion(),
            originalParameters
        );
        this.useLuceneBasedVectorField = KNNVectorFieldMapperUtil.useLuceneKNNVectorsFormat(indexCreatedVersion);
        KNNEngine knnEngine = knnLibraryIndex.getKnnLibraryIndexConfig().getKnnEngine();
        QuantizationConfig quantizationConfig = knnLibraryIndex.getQuantizationConfig();

        this.fieldType = new FieldType(KNNVectorFieldMapper.Defaults.FIELD_TYPE);
        this.fieldType.putAttribute(DIMENSION, String.valueOf(knnLibraryIndex.getDimension()));
        this.fieldType.putAttribute(SPACE_TYPE, knnLibraryIndex.getSpaceType().getValue());
        // Conditionally add quantization config
        if (quantizationConfig != null && quantizationConfig != QuantizationConfig.EMPTY) {
            this.fieldType.putAttribute(QFRAMEWORK_CONFIG, QuantizationConfigParser.toCsv(quantizationConfig));
        }

        this.fieldType.putAttribute(VECTOR_DATA_TYPE_FIELD, mappedFieldType.getVectorDataType().getValue());
        this.fieldType.putAttribute(KNN_ENGINE, knnEngine.getName());

        try {
            this.fieldType.putAttribute(PARAMETERS, XContentFactory.jsonBuilder().map(knnLibraryIndex.getLibraryParameters()).toString());
        } catch (IOException ioe) {
            throw new RuntimeException(String.format("Unable to create KNNVectorFieldMapper: %s", ioe));
        }

        if (useLuceneBasedVectorField) {
            int adjustedDimension = knnLibraryIndex.getVectorDataType() == VectorDataType.BINARY
                ? knnLibraryIndex.getDimension() / 8
                : knnLibraryIndex.getDimension();
            final VectorEncoding encoding = knnLibraryIndex.getVectorDataType() == VectorDataType.FLOAT
                ? VectorEncoding.FLOAT32
                : VectorEncoding.BYTE;
            fieldType.setVectorAttributes(
                adjustedDimension,
                encoding,
                SpaceType.DEFAULT.getKnnVectorSimilarityFunction().getVectorSimilarityFunction()
            );
        } else {
            fieldType.setDocValuesType(DocValuesType.BINARY);
        }

        this.fieldType.freeze();
        this.perDimensionProcessor = knnLibraryIndex.getPerDimensionProcessor();
        this.perDimensionValidator = knnLibraryIndex.getPerDimensionValidator();
        this.vectorValidator = knnLibraryIndex.getVectorValidator();
    }

    @Override
    protected VectorValidator getVectorValidator() {
        return vectorValidator;
    }

    @Override
    protected PerDimensionValidator getPerDimensionValidator() {
        return perDimensionValidator;
    }

    @Override
    protected PerDimensionProcessor getPerDimensionProcessor() {
        return perDimensionProcessor;
    }
}
