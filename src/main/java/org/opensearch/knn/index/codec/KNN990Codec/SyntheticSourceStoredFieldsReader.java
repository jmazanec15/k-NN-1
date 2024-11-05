/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;

@RequiredArgsConstructor
public class SyntheticSourceStoredFieldsReader extends StoredFieldsReader {
    private final StoredFieldsReader delegate;
    // Given docId and source, process source
    private final SyntheticVectorInjectionConsumer syntheticVectorInjectionConsumer;

    @Setter
    private boolean shouldInject = true;

    @Override
    public void document(int docId, StoredFieldVisitor storedFieldVisitor) throws IOException {
        if (shouldInject) {
            delegate.document(
                docId,
                new SyntheticSourceStoredFieldVisitor(storedFieldVisitor, bytes -> syntheticVectorInjectionConsumer.apply(docId, bytes))
            );
            return;
        }
        delegate.document(docId, storedFieldVisitor);
    }

    @Override
    public StoredFieldsReader clone() {
        return new SyntheticSourceStoredFieldsReader(delegate.clone(), syntheticVectorInjectionConsumer);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    public static StoredFieldsReader wrapForMerge(StoredFieldsReader storedFieldsReader) {
        if (storedFieldsReader instanceof SyntheticSourceStoredFieldsReader) {
            StoredFieldsReader storedFieldsReaderClone = storedFieldsReader.clone();
            ((SyntheticSourceStoredFieldsReader) storedFieldsReaderClone).setShouldInject(false);
            return storedFieldsReaderClone;
        }
        return storedFieldsReader;
    }
}
