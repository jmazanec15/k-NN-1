/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;

import java.io.IOException;
import java.util.function.Supplier;

public class SyntheticVectorInjector {

    private final Supplier<KnnVectorsReader> knnVectorsReaderSupplier;
    private final Supplier<DocValuesProducer> docValuesProducerSupplier;
    private final Supplier<ParentChildHelper> parentChildHelperSupplier;
    private final SegmentReadState segmentReadState;

    public SyntheticVectorInjector(
        Supplier<KnnVectorsReader> knnVectorsReaderSupplier,
        Supplier<DocValuesProducer> docValuesProducerSupplier,
        Supplier<FieldsProducer> fieldsProducerSupplier,
        SegmentReadState segmentReadState
    ) {
        this.docValuesProducerSupplier = docValuesProducerSupplier;
        this.knnVectorsReaderSupplier = knnVectorsReaderSupplier;
        this.parentChildHelperSupplier = () -> new ParentChildHelper(fieldsProducerSupplier.get(), docValuesProducerSupplier.get());
        this.segmentReadState = segmentReadState;
    }

    PerFieldSyntheticVectorInjector getPerFieldSyntheticVectorInjector(FieldInfo fieldInfo) {
        return new PerFieldSyntheticVectorInjector(() -> {
            try {
                return KNNVectorValuesFactory.getVectorValues(fieldInfo, docValuesProducerSupplier.get(), knnVectorsReaderSupplier.get());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, () -> {
            try {
                return parentChildHelperSupplier.get()
                    .getParentChildIterator(fieldInfo.name, segmentReadState.fieldInfos.fieldInfo("_primary_term"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, fieldInfo.getName());
    }
}
