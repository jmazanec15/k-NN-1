/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;

import java.io.IOException;
import java.util.function.Function;

/**
 * Injector class for nested fields and object fields.
 */
@Log4j2
public class NestedPerFieldDerivedVectorInjector extends AbstractPerFieldDerivedVectorInjector {

    private final FieldInfo childFieldInfo;
    private final DerivedSourceReaders derivedSourceReaders;

    /**
     *
     * @param childFieldInfo FieldInfo of the child field
     * @param derivedSourceReaders Readers for access segment info
     */
    public NestedPerFieldDerivedVectorInjector(
        FieldInfo childFieldInfo,
        DerivedSourceReaders derivedSourceReaders) {
        this.childFieldInfo = childFieldInfo;
        this.derivedSourceReaders = derivedSourceReaders;
    }

    @Override
    public Function<Object, Object> createTransformer(int rootDocId, int firstChild) throws IOException {
        KNNVectorValues<?> vectorValues = KNNVectorValuesFactory.getVectorValues(
                childFieldInfo,
                derivedSourceReaders.getDocValuesProducer(),
                derivedSourceReaders.getKnnVectorsReader()
        );
        vectorValues.advance(firstChild);
        return o -> {
            if (o == null) {
                return null;
            }
            try {
                Object vector = formatVector(
                        childFieldInfo,
                        vectorValues::getVector,
                        vectorValues::conditionalCloneVector
                );
                vectorValues.nextDoc();
                return vector;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
