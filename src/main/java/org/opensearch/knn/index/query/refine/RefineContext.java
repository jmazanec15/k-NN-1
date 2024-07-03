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

import lombok.Builder;
import lombok.Getter;
import org.opensearch.index.fielddata.IndexFieldData;

@Builder
@Getter
public class RefineContext {
    private final IndexFieldData<?> indexFieldData;
    private final Scorer<float[]> scorer;

    /**
     * Refine score using index vector
     *
     * @param indexVector vector to be rescored against query vector
     * @return new, higher precision score
     */
    public float refine(float[] indexVector) {
        return scorer.score(indexVector);
    }
}
