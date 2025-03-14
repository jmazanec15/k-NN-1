/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import org.apache.lucene.index.FieldInfo;

/**
 * Factory for creating {@link PerFieldDerivedVectorInjector} instances.
 */
class PerFieldDerivedVectorInjectorFactory {

    /**
     * Create a {@link PerFieldDerivedVectorInjector} instance based on information in field info.
     *
     * @param fieldInfo FieldInfo for the field to create the injector for
     * @param derivedSourceReaders {@link DerivedSourceReaders} instance
     * @return PerFieldDerivedVectorInjector instance
     */
    public static PerFieldDerivedVectorInjector create(
        FieldInfo fieldInfo,
        boolean isNested,
        DerivedSourceReaders derivedSourceReaders
    ) {
        // Nested case
        if (isNested) {
            return new NestedPerFieldDerivedVectorInjector(fieldInfo, derivedSourceReaders);
        }

        // Non-nested case
        return new RootPerFieldDerivedVectorInjector(fieldInfo, derivedSourceReaders);
    }
}
