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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.opensearch.knn.index.KNNVectorScriptDocValues;

import java.io.IOException;

/**
 * Calculates the refinement score on top of existing scorer.
 */
public class RefineScorer extends Scorer {
    private final Scorer subQueryScorer;
    private final RefineContext refineContext;
    private final KNNVectorScriptDocValues knnVectorScriptDocValues;

    /**
     *
     * @param weight parent weight
     * @param subQueryScorer scorer for wrapped Query
     * @param refineContext context for refinement logic
     * @param knnVectorScriptDocValues script doc values for KNN vector
     */
    protected RefineScorer(
        Weight weight,
        Scorer subQueryScorer,
        RefineContext refineContext,
        KNNVectorScriptDocValues knnVectorScriptDocValues
    ) {
        super(weight);
        this.subQueryScorer = subQueryScorer;
        this.refineContext = refineContext;
        this.knnVectorScriptDocValues = knnVectorScriptDocValues;
    }

    @Override
    public float score() throws IOException {
        knnVectorScriptDocValues.setNextDocId(docID());
        return refineContext.refine(knnVectorScriptDocValues.getValue());
    }

    @Override
    public int docID() {
        return subQueryScorer.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return subQueryScorer.iterator();
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        return subQueryScorer.twoPhaseIterator();
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        return subQueryScorer.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) {
        return Float.MAX_VALUE;
    }
}
