/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedPerFieldDerivedVectorInjector implements PerFieldDerivedVectorInjector {

    private final CheckedSupplier<KNNVectorValues<?>, IOException> vectorValuesSupplier;
    private final String parentField;
    private final String childField;
    private final CheckedSupplier<ParentChildIterator, IOException> parentChildIteratorSupplier;

    public NestedPerFieldDerivedVectorInjector(
        FieldInfo fieldInfo,
        DerivedSourceReaders derivedSourceReaders,
        SegmentReadState segmentReadState
    ) {
        this.vectorValuesSupplier = () -> KNNVectorValuesFactory.getVectorValues(
            fieldInfo,
            derivedSourceReaders.getDocValuesProducer(),
            derivedSourceReaders.getKnnVectorsReader()
        );

        this.parentField = ParentChildHelper.getParentField(fieldInfo.name);
        this.childField = ParentChildHelper.getChildField(fieldInfo.name);
        this.parentChildIteratorSupplier = () -> ParentChildIterator.getParentChildIterator(
            fieldInfo,
            segmentReadState,
            derivedSourceReaders.getDocValuesProducer(),
            derivedSourceReaders.getFieldsProducer()
        );
    }

    @Override
    public void inject(Integer docId, Map<String, Object> sourceAsMap) throws IOException {
        KNNVectorValues<?> vectorValues = vectorValuesSupplier.get();
        try {
            processNestedField(vectorValues, docId, sourceAsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processNestedField(KNNVectorValues<?> vectorValues, int parentDocId, Map<String, Object> sourceAsMap) throws IOException {
        // The task is defined as follows: Given a parent doc id and the source, we need to add all of the children
        // vectors as maps into the source. To do this, we need for a given parentDocId,

        // When reconstructing nested source, we will always reconstruct as a list of size number of children
        ParentChildIterator parentChildIterator = parentChildIteratorSupplier.get();
        int numberOfChildren = parentChildIterator.numChildren(parentDocId);
        int child = parentChildIterator.firstChild(parentDocId);
        int firstChild = child;

        // Initialize the individual maps of the children
        List<Map<String, ?>> reconstructedSource = new ArrayList<>(numberOfChildren);
        for (int i = 0; i < numberOfChildren; i++) {
            reconstructedSource.add(null);
        }
        initialize(sourceAsMap.get(parentField), reconstructedSource, firstChild);

        // At this point, we fill in the vectors
        while (child != -1) {
            // If the child does not have a vector, vectValues advance will advance past child to the next matching
            // docId. So, we need to ensure that doing this does not pass the parent docId.
            vectorValues.advance(child);
            int docId = vectorValues.docId();
            if (docId >= parentDocId) {
                break;
            }
            int childOffset = docId - firstChild;
            @SuppressWarnings("unchecked")
            Map<String, Object> childMap = (Map<String, Object>) reconstructedSource.get(childOffset);
            childMap.put(childField, vectorValues.conditionalCloneVector());
            child = parentChildIterator.nextChild(docId, parentDocId);
        }
        sourceAsMap.put(parentField, reconstructedSource);
    }

    // This function sets up all the maps in the list so that only the vectors need to be added
    private void initialize(Object existingComponent, List<Map<String, ?>> reconstructedList, int firstChild) throws IOException {
        // If the component doesnt exist, just initialize to an empty map
        if (existingComponent == null) {
            reconstructedList.replaceAll(ignored -> new HashMap<>());
            return;
        }

        // If the component is a map, then only one element was left after source filtering. We need to find where
        // it is based off of the missing vectors and then add it.
        if (existingComponent instanceof Map) {
            reconstructedList.replaceAll(ignored -> new HashMap<>());
            int firstDoc = findFirstSpotWhereVectorIsMissing(firstChild, 0, reconstructedList.size());
            reconstructedList.set(firstDoc, new HashMap<>((Map<String, Object>) existingComponent));
        }

        if (existingComponent instanceof List) {
            reconstructedList.replaceAll(ignored -> new HashMap<>());
            List<Map<String, Object>> currentList = (List<Map<String, Object>>) existingComponent;
            for (int i = 0; i < currentList.size(); i++) {
                int firstDoc = findFirstSpotWhereVectorIsMissing(firstChild, i, reconstructedList.size());
                reconstructedList.set(firstDoc, new HashMap<>(currentList.get(i)));
            }
        }
    }

    private int findFirstSpotWhereVectorIsMissing(int firstChild, int listOffset, int totalSize) throws IOException {
        KNNVectorValues<?> vectorValues = vectorValuesSupplier.get();
        vectorValues.advance(firstChild);
        int nonVecFields = 0;
        for (int i = 0; i < totalSize; i++) {
            if (vectorValues.docId() != i + firstChild) {
                if (nonVecFields++ == listOffset) {
                    return i;
                }
            } else {
                vectorValues.nextDoc();
            }
        }
        // Theres a vector for each doc to be injected
        if (vectorValues.docId() != firstChild) {
            return 0;
        }
        return -1;
    }
}
