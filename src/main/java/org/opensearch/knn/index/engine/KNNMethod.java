/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import org.opensearch.common.ValidationException;

/**
 * KNNMethod defines the structure of a method supported by a particular k-NN library. It is used to validate
 * the KNNMethodContext passed in by the user, where the KNNMethodContext provides the configuration that the user may
 * want. Then, it provides the information necessary to build and search engine knn indices.
 */
public interface KNNMethod {
    /**
     * Validate that the configured KNNMethodContext is valid for this method
     *
     * @param knnLibraryIndexConfig parameters that have been resolved from the user input
     * @param builder TODO: Fix
     * @throws ValidationException produced by validation errors; null if no validations errors.
     */
    void resolve(KNNLibraryIndexConfig knnLibraryIndexConfig, KNNLibraryIndex.Builder builder);
}
