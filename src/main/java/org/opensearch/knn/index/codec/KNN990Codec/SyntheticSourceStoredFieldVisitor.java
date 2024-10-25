/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;
import java.util.function.Function;

@AllArgsConstructor
public class SyntheticSourceStoredFieldVisitor extends StoredFieldVisitor {

    private final StoredFieldVisitor delegate;
    private final Function<byte[], byte[]> sourceModifier;

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        if (fieldInfo.name.equals("_source")) {
            delegate.binaryField(fieldInfo, sourceModifier.apply(value));
            return;
        }
        delegate.binaryField(fieldInfo, value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return delegate.needsField(fieldInfo);
    }
}
