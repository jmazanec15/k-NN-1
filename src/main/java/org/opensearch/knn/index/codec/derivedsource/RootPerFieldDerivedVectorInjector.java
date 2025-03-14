/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;

import java.io.IOException;
import java.util.function.Function;

/**
 * {@link PerFieldDerivedVectorInjector} for root fields (i.e. non nested fields).
 */
class RootPerFieldDerivedVectorInjector extends AbstractPerFieldDerivedVectorInjector {

    private final FieldInfo fieldInfo;
    private final CheckedSupplier<KNNVectorValues<?>, IOException> vectorValuesSupplier;

    /**
     * Constructor for RootPerFieldDerivedVectorInjector.
     *
     * @param fieldInfo FieldInfo for the field to create the injector for
     * @param derivedSourceReaders {@link DerivedSourceReaders} instance
     */
    public RootPerFieldDerivedVectorInjector(FieldInfo fieldInfo, DerivedSourceReaders derivedSourceReaders) {
        this.fieldInfo = fieldInfo;
        this.vectorValuesSupplier = () -> KNNVectorValuesFactory.getVectorValues(
            fieldInfo,
            derivedSourceReaders.getDocValuesProducer(),
            derivedSourceReaders.getKnnVectorsReader()
        );
    }

    @Override
    public Function<Object, Object> createTransformer(int rootDocId, int firstChild) throws IOException {
        KNNVectorValues<?> vectorValues = vectorValuesSupplier.get();
        vectorValues.advance(rootDocId);
        return o -> {
            if (o == null || vectorValues.docId() > rootDocId) {
                return null;
            }

            try {
                return formatVector(fieldInfo, vectorValues::getVector, vectorValues::conditionalCloneVector);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
