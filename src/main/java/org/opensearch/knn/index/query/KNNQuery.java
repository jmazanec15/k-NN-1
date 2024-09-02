/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.query.rescore.RescoreContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Custom KNN query. Query is used for KNNEngine's that create their own custom segment files. These files need to be
 * loaded and queried in a custom manner throughout the query path.
 */
@Getter
@Builder
@AllArgsConstructor
public class KNNQuery extends Query {

    private final String field;
    private final float[] queryVector;
    private final byte[] byteQueryVector;
    private int k;
    private Map<String, ?> methodParameters;
    private final String indexName;
    private final RescoreContext rescoreContext;

    private final VectorDataType vectorDataType;
    private final SpaceType spaceType;
    private final KNNEngine knnEngine;
    private final String modelId;

    @Setter
    private Query filterQuery;
    private BitSetProducer parentsFilter;
    private Float radius;
    private Context context;

    /**
     * Constructs Weight implementation for this query
     *
     * @param searcher  searcher for given segment
     * @param scoreMode  How the produced scorers will be consumed.
     * @param boost     The boost that is propagated by the parent queries.
     * @return Weight   For calculating scores
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (!KNNSettings.isKNNPluginEnabled()) {
            throw new IllegalStateException("KNN plugin is disabled. To enable update knn.plugin.enabled to true");
        }
        final Weight filterWeight = getFilterWeight(searcher);
        if (filterWeight != null) {
            return new KNNWeight(this, boost, filterWeight);
        }
        return new KNNWeight(this, boost);
    }

    private Weight getFilterWeight(IndexSearcher searcher) throws IOException {
        if (this.getFilterQuery() != null) {
            // Run the filter query
            final BooleanQuery booleanQuery = new BooleanQuery.Builder().add(this.getFilterQuery(), BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(this.getField()), BooleanClause.Occur.FILTER)
                .build();
            final Query rewritten = searcher.rewrite(booleanQuery);
            return searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        return null;
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public String toString(String field) {
        return field;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field,
            Arrays.hashCode(queryVector),
            k,
            indexName,
            filterQuery,
            context,
            parentsFilter,
            radius,
            methodParameters,
            rescoreContext
        );
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(KNNQuery other) {
        if (other == this) return true;
        return Objects.equals(field, other.field)
            && Arrays.equals(queryVector, other.queryVector)
            && Arrays.equals(byteQueryVector, other.byteQueryVector)
            && Objects.equals(k, other.k)
            && Objects.equals(methodParameters, other.methodParameters)
            && Objects.equals(radius, other.radius)
            && Objects.equals(context, other.context)
            && Objects.equals(indexName, other.indexName)
            && Objects.equals(parentsFilter, other.parentsFilter)
            && Objects.equals(rescoreContext, other.rescoreContext)
            && Objects.equals(filterQuery, other.filterQuery);
    }

    /**
     * Context for KNNQuery
     */
    @Setter
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Context {
        int maxResultWindow;
    }
}
