/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.Getter;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class PerFieldSyntheticVectorInjector implements BiConsumer<Integer, Map<String, Object>> {

    Supplier<KNNVectorValues<?>> vectorValuesSupplier;
    Supplier<ParentChildHelper.ParentChildIterator> parentChildIteratorSupplier;
    ParentChildHelper.ParentChildIterator parentChildIterator;
    @Getter
    String fieldName;

    public PerFieldSyntheticVectorInjector(
        Supplier<KNNVectorValues<?>> vectorValuesSupplier,
        Supplier<ParentChildHelper.ParentChildIterator> parentChildIteratorSupplier,
        String fieldName
    ) {
        this.vectorValuesSupplier = vectorValuesSupplier;
        this.parentChildIteratorSupplier = parentChildIteratorSupplier;
        this.fieldName = fieldName;
    }

    private synchronized void init() {
        if (parentChildIterator != null) return;
        parentChildIterator = parentChildIteratorSupplier.get();
    }

    @Override
    public void accept(Integer docId, Map<String, Object> sourceAsMap) {
        KNNVectorValues<?> vectorValues = vectorValuesSupplier.get();
        init();
        try {
            if (parentChildIterator != null) {
                processNestedField(vectorValues, docId, sourceAsMap);
            } else {
                vectorValues.advance(docId);
                sourceAsMap.put(fieldName, vectorValues.getVector());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processNestedField(KNNVectorValues<?> vectorValues, int parentDocId, Map<String, Object> sourceAsMap) throws IOException {
        String childFieldName = ParentChildHelper.getChildField(fieldName);
        int child = parentChildIterator.firstChild(parentDocId);
        int firstChild = child;
        List<?> fieldsArray = maybeCreateParentArray(fieldName, sourceAsMap, parentDocId - firstChild);
        while (child != -1) {
            vectorValues.advance(child);
            Map<String, Object> childMap = (Map<String, Object>) fieldsArray.get(child - firstChild);
            childMap.put(childFieldName, vectorValues.conditionalCloneVector());
            child = parentChildIterator.nextChild(child, parentDocId);
        }
    }

    private List<?> maybeCreateParentArray(String fieldName, Map<String, Object> source, int initSize) {
        List<Map<String, ?>> list = (List<Map<String, ?>>) source.computeIfAbsent(
            ParentChildHelper.getParentField(fieldName),
            k -> new ArrayList<>()
        );
        if (list.size() == 0) {
            for (int i = 0; i < initSize; i++) {
                list.add(new HashMap<>());
            }
        }
        return list;
    }
}
