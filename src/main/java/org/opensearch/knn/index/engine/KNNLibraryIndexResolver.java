/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

//TODO: remove this class or merge with KNNEngineResolver
public final class KNNLibraryIndexResolver {

    public static KNNLibraryIndex resolve(KNNLibraryIndexConfig knnLibraryIndexConfig) {
        return knnLibraryIndexConfig.getKnnEngine().resolve(knnLibraryIndexConfig);
    }
}
