/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.query.rescore.RescoreContext;

import java.util.Map;

/**
 * Class is used to resolve parameters used during search for a given {@link KNNLibraryIndex}.
 */
public interface KNNLibraryIndexSearchResolver {
    /**
     * Resolves the search-time parameters a user passes in
     *
     * @param ctx QueryContext
     * @param userParameters Map of user parameters
     * @return processed parameters
     */
    default Map<String, Object> resolveMethodParameters(QueryContext ctx, Map<String, Object> userParameters) {
        return userParameters;
    }

    /**
     * Resolves the rescore context a user passes in
     *
     * @param ctx QueryContext
     * @param userRescoreContext RescoreContext
     * @return processed rescore context
     */
    default RescoreContext resolveRescoreContext(QueryContext ctx, RescoreContext userRescoreContext) {
        return userRescoreContext;
    }

    Float resolveRadius(QueryContext ctx, Float maxDistance, Float minScore);

    byte[] resolveByteQueryVector(QueryContext ctx, float[] queryVector);

    float[] resolveFloatQueryVector(QueryContext ctx, float[] queryVector);

    QueryBuilder resolveFilter(QueryContext ctx, QueryBuilder filter);
}
