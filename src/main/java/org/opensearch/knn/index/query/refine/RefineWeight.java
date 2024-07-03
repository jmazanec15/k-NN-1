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

import lombok.Getter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.knn.index.KNNVectorScriptDocValues;

import java.io.IOException;

/**
 * Weight implementation for refining results of a query with higher precision computations.
 * <p>
 * TODO:
 *  - Properly explain
 *  - Caching
 *  - Double check on bulk scorer/score supplier interfaces
 */
@Getter
public class RefineWeight extends Weight {
    private final Weight subQueryWeight;
    private final RefineContext refineContext;

    /**
     *
     * @param query Parent query
     * @param subQueryWeight wrapped query weight function
     * @param refineContext context for re-scoring
     */
    protected RefineWeight(Query query, Weight subQueryWeight, RefineContext refineContext) {
        super(query);
        this.subQueryWeight = subQueryWeight;
        this.refineContext = refineContext;
    }

    @Override
    public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
        return null;
    }

    @Override
    public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
        Scorer subQueryScorer = subQueryWeight.scorer(leafReaderContext);
        return new RefineScorer(this, subQueryScorer, refineContext, getScriptDocValues(leafReaderContext));
    }

    @Override
    public boolean isCacheable(LeafReaderContext leafReaderContext) {
        return false;
    }

    /**
     * Get script doc values from leaf reader context
     *
     * @param leafReaderContext leaf reader context
     * @return KNNVectorScriptDocValues
     */
    private KNNVectorScriptDocValues getScriptDocValues(LeafReaderContext leafReaderContext) {
        return (KNNVectorScriptDocValues) refineContext.getIndexFieldData().load(leafReaderContext).getScriptValues();
    }
}
