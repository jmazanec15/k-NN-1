/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.VectorQueryType;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.query.KNNQueryBuilder;

import java.util.Locale;

import static org.opensearch.knn.common.KNNValidationUtil.validateByteVectorValue;
import static org.opensearch.knn.index.engine.KNNEngine.ENGINES_SUPPORTING_RADIAL_SEARCH;

@AllArgsConstructor
public final class DefaultKNNLibraryIndexSearchResolver implements KNNLibraryIndexSearchResolver {

    KNNLibraryIndexConfig knnLibraryIndexConfig;

    @Override
    public Float resolveRadius(QueryContext ctx, Float maxDistance, Float minScore) {
        if (ctx.getQueryType() == VectorQueryType.K) {
            return null;
        }

        SpaceType spaceType = knnLibraryIndexConfig.getSpaceType();
        KNNEngine knnEngine = knnLibraryIndexConfig.getKnnEngine();
        VectorDataType vectorDataType = knnLibraryIndexConfig.getVectorDataType();

        if (vectorDataType == VectorDataType.BINARY) {
            throw new UnsupportedOperationException("Binary data type does not support radial search");
        }

        if (!ENGINES_SUPPORTING_RADIAL_SEARCH.contains(knnEngine)) {
            throw new UnsupportedOperationException(
                String.format(Locale.ROOT, "Engine [%s] does not support radial search", knnEngine.getName())
            );
        }

        if (maxDistance != null) {
            if (maxDistance < 0 && SpaceType.INNER_PRODUCT.equals(knnLibraryIndexConfig.getSpaceType()) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        "[%s] requires distance to be non-negative for space type: %s",
                        KNNQueryBuilder.NAME,
                        spaceType.getValue()
                    )
                );
            }
            return knnLibraryIndexConfig.getKnnEngine().distanceToRadialThreshold(maxDistance, spaceType);
        }

        if (minScore != null) {
            if (minScore > 1 && SpaceType.INNER_PRODUCT.equals(knnLibraryIndexConfig.getSpaceType()) == false) {
                throw new IllegalArgumentException(
                    String.format("[%s] requires score to be in the range [0, 1] for space type: %s", KNNQueryBuilder.NAME, spaceType)
                );
            }
            return knnEngine.scoreToRadialThreshold(minScore, spaceType);
        }
        return null;
    }

    @Override
    public float[] resolveFloatQueryVector(QueryContext ctx, float[] queryVector) {
        knnLibraryIndexConfig.getSpaceType().validateVector(queryVector);
        return queryVector;
    }

    @Override
    public byte[] resolveByteQueryVector(QueryContext ctx, float[] queryVector) {
        byte[] byteVector = new byte[0];
        SpaceType spaceType = knnLibraryIndexConfig.getSpaceType();
        VectorDataType vectorDataType = knnLibraryIndexConfig.getVectorDataType();
        KNNEngine knnEngine = knnLibraryIndexConfig.getKnnEngine();
        switch (knnLibraryIndexConfig.getVectorDataType()) {
            case BINARY:
                byteVector = new byte[queryVector.length];
                for (int i = 0; i < queryVector.length; i++) {
                    validateByteVectorValue(queryVector[i], vectorDataType);
                    byteVector[i] = (byte) queryVector[i];
                }
                spaceType.validateVector(byteVector);
                break;
            case BYTE:
                if (KNNEngine.LUCENE == knnEngine) {
                    byteVector = new byte[queryVector.length];
                    for (int i = 0; i < queryVector.length; i++) {
                        validateByteVectorValue(queryVector[i], vectorDataType);
                        byteVector[i] = (byte) queryVector[i];
                    }
                    spaceType.validateVector(byteVector);
                } else {
                    for (float v : queryVector) {
                        validateByteVectorValue(v, vectorDataType);
                    }
                    spaceType.validateVector(queryVector);
                }
                break;
            default:
                throw new IllegalStateException("Invalid type for byte query vector");
        }
        return byteVector;
    }

    @Override
    public QueryBuilder resolveFilter(QueryContext ctx, QueryBuilder filter) {
        if (KNNEngine.getEnginesThatCreateCustomSegmentFiles().contains(knnLibraryIndexConfig.getKnnEngine())
            && filter != null
            && !KNNEngine.getEnginesThatSupportsFilters().contains(knnLibraryIndexConfig.getKnnEngine())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Engine [%s] does not support filters", knnLibraryIndexConfig.getKnnEngine())
            );
        }
        return filter;
    }
}
