/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.query.rescore.RescoreContext;

import java.util.Map;

@AllArgsConstructor
public abstract class FilterKNNLibraryIndexSearchResolver implements KNNLibraryIndexSearchResolver {
    private final KNNLibraryIndexSearchResolver delegate;

    @Override
    public Map<String, Object> resolveMethodParameters(QueryContext ctx, Map<String, Object> userParameters) {
        return delegate.resolveMethodParameters(ctx, userParameters);
    }

    @Override
    public RescoreContext resolveRescoreContext(QueryContext ctx, RescoreContext userRescoreContext) {
        return delegate.resolveRescoreContext(ctx, userRescoreContext);
    }

    @Override
    public Float resolveRadius(QueryContext ctx, Float maxDistance, Float minScore) {
        return delegate.resolveRadius(ctx, maxDistance, minScore);
    }

    @Override
    public byte[] resolveByteQueryVector(QueryContext ctx, float[] queryVector) {
        return delegate.resolveByteQueryVector(ctx, queryVector);
    }

    @Override
    public float[] resolveFloatQueryVector(QueryContext ctx, float[] queryVector) {
        return delegate.resolveFloatQueryVector(ctx, queryVector);
    }

    @Override
    public QueryBuilder resolveFilter(QueryContext ctx, QueryBuilder filter) {
        return delegate.resolveFilter(ctx, filter);
    }
}
