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

public interface Refiner {
    /**
     * Refine the result.
     *
     * @param indexVector Index vector
     * @return Refined score between 2 vectors
     */
    float score(float[] indexVector);
}
