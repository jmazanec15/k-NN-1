/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.opensearch.knn.index.codec.util.KNNVectorSerializer;
import org.opensearch.knn.index.codec.util.KNNVectorSerializerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;

/**
 * Enum contains data_type of vectors and right now only supported for lucene engine in k-NN plugin.
 * We have two vector data_types, one is float (default) and the other one is byte.
 */
@AllArgsConstructor
public enum VectorDataType {
    BYTE("byte") {

        @Override
        public FieldType createKnnVectorFieldType(int dimension, VectorSimilarityFunction vectorSimilarityFunction) {
            return KnnByteVectorField.createFieldType(dimension, vectorSimilarityFunction);
        }

        @Override
        public float[] getVectorFromBytesRef(BytesRef binaryValue) {
            float[] vector = new float[binaryValue.length];
            int i = 0;
            int j = binaryValue.offset;

            while (i < binaryValue.length) {
                vector[i++] = binaryValue.bytes[j++];
            }
            return vector;
        }

        @Override
        public void getVectorFromBytesRef(BytesRef binaryValue, float[] destination) {
            throw new UnsupportedOperationException("Not supported yet");
        }
    },
    FLOAT("float") {

        @Override
        public FieldType createKnnVectorFieldType(int dimension, VectorSimilarityFunction vectorSimilarityFunction) {
            return KnnVectorField.createFieldType(dimension, vectorSimilarityFunction);
        }

        @Override
        public float[] getVectorFromBytesRef(BytesRef binaryValue) {
            final KNNVectorSerializer vectorSerializer = KNNVectorSerializerFactory.getSerializerBySerializationMode(
                KNNVectorSerializerFactory.serializerModeByteRef(binaryValue)
            );
            return vectorSerializer.byteToFloatArray(binaryValue);
        }

        @Override
        public void getVectorFromBytesRef(BytesRef binaryValue, float[] destination) {
            // TODO: Hardcoding
            final KNNVectorSerializer vectorSerializer = KNNVectorSerializerFactory.getDefaultSerializer();
            vectorSerializer.byteToFloatArray(binaryValue, destination);
        }
    };

    public static final String SUPPORTED_VECTOR_DATA_TYPES = Arrays.stream(VectorDataType.values())
        .map(VectorDataType::getValue)
        .collect(Collectors.joining(","));
    @Getter
    private final String value;

    /**
     * Creates a KnnVectorFieldType based on the VectorDataType using the provided dimension and
     * VectorSimilarityFunction.
     *
     * @param dimension Dimension of the vector
     * @param vectorSimilarityFunction VectorSimilarityFunction for a given spaceType
     * @return FieldType
     */
    public abstract FieldType createKnnVectorFieldType(int dimension, VectorSimilarityFunction vectorSimilarityFunction);

    /**
     * Deserializes float vector from BytesRef.
     *
     * @param binaryValue Binary Value
     * @return float vector deserialized from binary value
     */
    public abstract float[] getVectorFromBytesRef(BytesRef binaryValue);

    public abstract void getVectorFromBytesRef(BytesRef binaryValue, float[] destination);

    /**
     * Validates if given VectorDataType is in the list of supported data types.
     * @param vectorDataType VectorDataType
     * @return  the same VectorDataType if it is in the supported values
     * throws Exception if an invalid value is provided.
     */
    public static VectorDataType get(String vectorDataType) {
        Objects.requireNonNull(
            vectorDataType,
            String.format(
                Locale.ROOT,
                "[%s] should not be null. Supported types are [%s]",
                VECTOR_DATA_TYPE_FIELD,
                SUPPORTED_VECTOR_DATA_TYPES
            )
        );
        try {
            return VectorDataType.valueOf(vectorDataType.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Invalid value provided for [%s] field. Supported values are [%s]",
                    VECTOR_DATA_TYPE_FIELD,
                    SUPPORTED_VECTOR_DATA_TYPES
                )
            );
        }
    }
}
