/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

@AllArgsConstructor
public class SyntheticSourceStoredFieldsFormat extends StoredFieldsFormat {

    private final StoredFieldsFormat delegate;
    private final Function<SegmentReadState, Function<FieldInfo, KNNVectorValues<?>>> vectorValuesSupplierByFieldSupplier;

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext ioContext)
        throws IOException {
        // If any field has this value set, than get its supplier
        Function<FieldInfo, KNNVectorValues<?>> vectorValuesSupplierByField = vectorValuesSupplierByFieldSupplier.apply(
            new SegmentReadState(directory, segmentInfo, fieldInfos, ioContext)
        );
        Map<String, Supplier<KNNVectorValues<?>>> vectorValuesSuppliers = new HashMap<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            if (Boolean.parseBoolean(fieldInfo.attributes().get(KNNVectorFieldMapper.KNN_FIELD))) {
                vectorValuesSuppliers.put(fieldInfo.name, () -> vectorValuesSupplierByField.apply(fieldInfo));
            }
        }

        // Build the processor and create the reader
        SyntheticVectorInjectionConsumer syntheticVectorInjectionConsumer = new SyntheticVectorInjectionConsumer(vectorValuesSuppliers);
        return new SyntheticSourceStoredFieldsReader(
            delegate.fieldsReader(directory, segmentInfo, fieldInfos, ioContext),
            syntheticVectorInjectionConsumer
        );
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        return delegate.fieldsWriter(directory, segmentInfo, ioContext);
    }
}
