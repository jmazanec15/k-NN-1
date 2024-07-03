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
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.query.KNNWeight;
import org.opensearch.knn.plugin.script.KNNScoringUtil;

import static org.opensearch.knn.plugin.script.KNNScoringSpaceUtil.getVectorMagnitudeSquared;

@AllArgsConstructor
public abstract class AbstractScorer<T> implements Scorer<T> {
    protected T queryVector;

    public static class L2Scorer extends AbstractScorer<float[]> {
        /**
         * @param queryVector vector used for query
         */
        public L2Scorer(float[] queryVector) {
            super(queryVector);
        }

        @Override
        public float score(float[] indexVector) {
            return 1 / (1 + KNNScoringUtil.l2Squared(queryVector, indexVector));
        }
    }

    public static class CosineSimilarityScorer extends AbstractScorer<float[]> {
        private final float qVectorSquaredMagnitude;

        /**
         * @param queryVector vector used for query
         */
        public CosineSimilarityScorer(float[] queryVector) {
            super(queryVector);
            SpaceType.COSINESIMIL.validateVector(queryVector);
            qVectorSquaredMagnitude = getVectorMagnitudeSquared(queryVector);
        }

        @Override
        public float score(float[] indexVector) {
            return 1 + KNNScoringUtil.cosinesimilOptimized(queryVector, indexVector, qVectorSquaredMagnitude);
        }
    }

    public static class InnerProductScorer extends AbstractScorer<float[]> {
        /**
         * @param queryVector vector used for query
         */
        public InnerProductScorer(float[] queryVector) {
            super(queryVector);
        }

        @Override
        public float score(float[] indexVector) {
            return KNNWeight.normalizeScore(-KNNScoringUtil.innerProduct(queryVector, indexVector));
        }
    }

    public static class L1Scorer extends AbstractScorer<float[]> {
        /**
         * @param queryVector vector used for query
         */
        public L1Scorer(float[] queryVector) {
            super(queryVector);
        }

        @Override
        public float score(float[] indexVector) {
            return 1 / (1 + KNNScoringUtil.l1Norm(queryVector, indexVector));
        }
    }

    public static class LInfScorer extends AbstractScorer<float[]> {
        /**
         * @param queryVector vector used for query
         */
        public LInfScorer(float[] queryVector) {
            super(queryVector);
        }

        @Override
        public float score(float[] indexVector) {
            return 1 / (1 + KNNScoringUtil.lInfNorm(queryVector, indexVector));
        }
    }

    /**
     * Based on space type, create scorer
     *
     * @param spaceType type of space
     * @param queryVector vector for querying
     * @return Scorer
     */
    public static Scorer<float[]> createScorer(SpaceType spaceType, float[] queryVector) {
        if (spaceType == SpaceType.L2) {
            return new L2Scorer(queryVector);
        }

        if (spaceType == SpaceType.COSINESIMIL) {
            return new CosineSimilarityScorer(queryVector);
        }

        if (spaceType == SpaceType.INNER_PRODUCT) {
            return new InnerProductScorer(queryVector);
        }

        if (spaceType == SpaceType.L1) {
            return new L1Scorer(queryVector);
        }

        if (spaceType == SpaceType.LINF) {
            return new LInfScorer(queryVector);
        }

        throw new IllegalArgumentException("Invalid space type");
    }
}
