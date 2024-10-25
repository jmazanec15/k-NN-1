/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;
import java.util.function.BiFunction;

@AllArgsConstructor
public class SyntheticSourceStoredFieldsReader extends StoredFieldsReader {
    private final StoredFieldsReader delegate;
    // Given docId and source, process source
    private final BiFunction<Integer, byte[], byte[]> sourceModifier;

    @Override
    public void document(int docId, StoredFieldVisitor storedFieldVisitor) throws IOException {
        delegate.document(docId, new SyntheticSourceStoredFieldVisitor(storedFieldVisitor, bytes -> sourceModifier.apply(docId, bytes)));
    }

    @Override
    public StoredFieldsReader clone() {
        return new SyntheticSourceStoredFieldsReader(delegate.clone(), sourceModifier);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
