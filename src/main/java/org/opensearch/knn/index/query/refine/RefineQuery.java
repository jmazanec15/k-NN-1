/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.index.query.refine;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Query that runs a sub-query and then rescores based on the fuller precision
 * representation of those vectors.
 * <p>
 * Heavily inspired by {@link org.opensearch.common.lucene.search.function.ScriptScoreQuery}
 * </p>
 * <p>
 *  TODO:
 *      - fix toString
 *      - check visit method
 *      - fix equals
 *      - fix hashCode
 * </p>
 */
@AllArgsConstructor
@Getter
public class RefineQuery extends Query {
    private final Query subQuery;
    private final RefineContext refineContext;

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query newQ = subQuery.rewrite(searcher);
        if (newQ != subQuery) {
            return new RefineQuery(newQ, getRefineContext());
        }
        return super.rewrite(searcher);
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("refine (").append(this.subQuery.toString(field)).append(")");
        sb.append(", refine: ");
        sb.append("{" + "add_space_type" + "}");
        return sb.toString();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        subQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public boolean equals(Object o) {
        return subQuery.equals(o);
    }

    @Override
    public int hashCode() {
        return subQuery.hashCode();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight subQueryWeight = subQuery.createWeight(searcher, scoreMode, 1.0f);
        return new RefineWeight(this, subQueryWeight, refineContext);
    }
}
