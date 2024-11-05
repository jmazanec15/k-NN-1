/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.Getter;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class PerFieldSyntheticVectorInjector implements BiConsumer<Integer, Map<String, Object>> {

    Supplier<KNNVectorValues<?>> vectorValuesSupplier;
    @Getter
    String fieldName;

    public PerFieldSyntheticVectorInjector(Supplier<KNNVectorValues<?>> vectorValuesSupplier, String fieldName) {
        this.vectorValuesSupplier = vectorValuesSupplier;
        this.fieldName = fieldName;
    }

    @Override
    public void accept(Integer docId, Map<String, Object> sourceAsMap) {
        KNNVectorValues<?> vectorValues = vectorValuesSupplier.get();
        try {
            if (vectorValues.docId() == docId || vectorValues.advance(docId) == docId) {
                sourceAsMap.put(fieldName, vectorValues.getVector());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
