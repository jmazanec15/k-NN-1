/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.Explicit;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.VectorField;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNIndexContext;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.engine.UserProvidedParameters;

import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.createStoredFieldForByteVector;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.createStoredFieldForFloatVector;
import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.buildDocValuesFieldType;

/**
 * Field mapper for case when Lucene has been set as an engine.
 */
public class LuceneFieldMapper extends KNNVectorFieldMapper {

    /** FieldType used for initializing VectorField, which is used for creating binary doc values. **/
    private final FieldType vectorFieldType;

    private final PerDimensionProcessor perDimensionProcessor;
    private final PerDimensionValidator perDimensionValidator;
    private final VectorValidator vectorValidator;

    static LuceneFieldMapper createFieldMapper(
        String fullname,
        Map<String, String> metaValue,
        KNNIndexContext knnIndexContext,
        UserProvidedParameters originalParameters,
        CreateLuceneFieldMapperInput createLuceneFieldMapperInput
    ) {
        final KNNVectorFieldType mappedFieldType = new KNNVectorFieldType(
            fullname,
            metaValue,
            () -> KNNVectorFieldType.KNNVectorFieldTypeConfig.builder()
                .dimension(knnIndexContext.getDimension())
                .vectorDataType(knnIndexContext.getVectorDataType())
                .knnIndexContext(knnIndexContext)
                .spaceType(knnIndexContext.getSpaceType())
                .knnEngine(knnIndexContext.getKNNEngine())
                .build(),
            null
        );

        return new LuceneFieldMapper(mappedFieldType, createLuceneFieldMapperInput, knnIndexContext, originalParameters);
    }

    private LuceneFieldMapper(
        final KNNVectorFieldType mappedFieldType,
        final CreateLuceneFieldMapperInput input,
        KNNIndexContext knnIndexContext,
        UserProvidedParameters originalParameters
    ) {
        super(
            input.getName(),
            mappedFieldType,
            input.getMultiFields(),
            input.getCopyTo(),
            input.getIgnoreMalformed(),
            input.isStored(),
            input.isHasDocValues(),
            knnIndexContext.getCreatedVersion(),
            originalParameters
        );
        VectorDataType vectorDataType = knnIndexContext.getVectorDataType();

        final VectorSimilarityFunction vectorSimilarityFunction = knnIndexContext.getSpaceType()
            .getKnnVectorSimilarityFunction()
            .getVectorSimilarityFunction();

        this.fieldType = vectorDataType.createKnnVectorFieldType(knnIndexContext.getDimension(), vectorSimilarityFunction);

        KNNEngine knnEngine = knnIndexContext.getKNNEngine();
        if (this.hasDocValues) {
            this.vectorFieldType = buildDocValuesFieldType(knnEngine);
        } else {
            this.vectorFieldType = null;
        }

        this.perDimensionProcessor = knnIndexContext.getPerDimensionProcessor();
        this.perDimensionValidator = knnIndexContext.getPerDimensionValidator();
        this.vectorValidator = knnIndexContext.getVectorValidator();
    }

    @Override
    protected List<Field> getFieldsForFloatVector(final float[] array) {
        final List<Field> fieldsToBeAdded = new ArrayList<>();
        fieldsToBeAdded.add(new KnnFloatVectorField(name(), array, fieldType));

        if (hasDocValues && vectorFieldType != null) {
            fieldsToBeAdded.add(new VectorField(name(), array, vectorFieldType));
        }

        if (this.stored) {
            fieldsToBeAdded.add(createStoredFieldForFloatVector(name(), array));
        }
        return fieldsToBeAdded;
    }

    @Override
    protected List<Field> getFieldsForByteVector(final byte[] array) {
        final List<Field> fieldsToBeAdded = new ArrayList<>();
        fieldsToBeAdded.add(new KnnByteVectorField(name(), array, fieldType));

        if (hasDocValues && vectorFieldType != null) {
            fieldsToBeAdded.add(new VectorField(name(), array, vectorFieldType));
        }

        if (this.stored) {
            fieldsToBeAdded.add(createStoredFieldForByteVector(name(), array));
        }
        return fieldsToBeAdded;
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

    @Override
    void updateEngineStats() {
        KNNEngine.LUCENE.setInitialized(true);
    }

    @AllArgsConstructor
    @lombok.Builder
    @Getter
    static class CreateLuceneFieldMapperInput {
        @NonNull
        String name;
        @NonNull
        MultiFields multiFields;
        @NonNull
        CopyTo copyTo;
        @NonNull
        Explicit<Boolean> ignoreMalformed;
        boolean stored;
        boolean hasDocValues;
        KNNMethodContext originalKnnMethodContext;
    }
}
