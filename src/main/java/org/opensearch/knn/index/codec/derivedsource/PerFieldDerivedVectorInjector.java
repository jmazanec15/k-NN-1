/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import java.io.IOException;
import java.util.function.Function;

/**
 * Interface for providing a transformer for a given parent doc
 */
public interface PerFieldDerivedVectorInjector {

    /**
     * Create a transformer for a given  parent doc
     *
     * @param rootDocId  The root doc id of the parent doc
     * @param firstChild The first child doc id of the parent doc. -1 if not applicable
     * @return a function that takes an object and returns the transformed object
     */
    Function<Object, Object> createTransformer(int rootDocId, int firstChild) throws IOException;
}
