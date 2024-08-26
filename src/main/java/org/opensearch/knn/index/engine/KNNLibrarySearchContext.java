/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.query.rescore.RescoreContext;

import java.util.Map;

/**
 * Holds the context needed to search a knn library.
 */
public interface KNNLibrarySearchContext {

    Map<String, Object> processMethodParameters(QueryContext ctx, Map<String, Object> parameters);

    RescoreContext getDefaultRescoreContext(QueryContext ctx);

    KNNLibrarySearchContext EMPTY = new KNNLibrarySearchContext() {

        @Override
        public Map<String, Object> processMethodParameters(QueryContext ctx, Map<String, Object> parameters) {
            return parameters;
        }

        @Override
        public RescoreContext getDefaultRescoreContext(QueryContext ctx) {
            return null;
        }
    };
}
