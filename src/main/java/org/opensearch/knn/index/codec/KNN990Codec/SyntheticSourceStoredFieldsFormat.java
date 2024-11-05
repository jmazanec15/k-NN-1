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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@AllArgsConstructor
public class SyntheticSourceStoredFieldsFormat extends StoredFieldsFormat {

    private final StoredFieldsFormat delegate;
    private final Function<SegmentReadState, SyntheticVectorInjector> syntheticVectorInjectorSupplier;
    // IMPORTANT Do not rely on this for the reader, it will be null if SPI is used
    private final Optional<MapperService> mapperService;

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext ioContext)
        throws IOException {
        // If any field has this value set, than get its supplier
        SyntheticVectorInjector syntheticVectorInjector = syntheticVectorInjectorSupplier.apply(
            new SegmentReadState(directory, segmentInfo, fieldInfos, ioContext)
        );
        List<PerFieldSyntheticVectorInjector> perFieldSyntheticVectorInjectors = new ArrayList<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            if (Boolean.parseBoolean(fieldInfo.attributes().get("knn-syn-source"))) {
                perFieldSyntheticVectorInjectors.add(syntheticVectorInjector.getPerFieldSyntheticVectorInjector(fieldInfo));
            }
        }

        // Build the processor and create the reader
        SyntheticVectorInjectionConsumer syntheticVectorInjectionConsumer = new SyntheticVectorInjectionConsumer(
            perFieldSyntheticVectorInjectors
        );
        return new SyntheticSourceStoredFieldsReader(
            delegate.fieldsReader(directory, segmentInfo, fieldInfos, ioContext),
            syntheticVectorInjectionConsumer
        );
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        StoredFieldsWriter delegateWriter = delegate.fieldsWriter(directory, segmentInfo, ioContext);
        if (mapperService.isPresent() && KNNSettings.isKNNSyntheticSourceEnabled(mapperService.get().getIndexSettings().getSettings())) {
            List<String> vectorFieldTypes = new ArrayList<>();
            for (MappedFieldType fieldType : mapperService.get().fieldTypes()) {
                if (fieldType instanceof KNNVectorFieldType) {
                    vectorFieldTypes.add(fieldType.name());
                }
            }
            return new SyntheticSourceStoredFieldsWriter(delegateWriter, vectorFieldTypes);
        }
        return delegateWriter;
    }
}
