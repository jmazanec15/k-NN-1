/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.query.rescore;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public final class RescoreContext {

    public static final float DEFAULT_OVERSAMPLE_FACTOR = 1.0f;
    public static final float MAX_OVERSAMPLE_FACTOR = 100.0f;
    public static final float MIN_OVERSAMPLE_FACTOR = 1.0f;

    public static final int MAX_FIRST_PASS_RESULTS = 10000;

    // Todo:- We will improve this in upcoming releases
    public static final int MIN_FIRST_PASS_RESULTS = 100;

    public static final RescoreContext EXPLICITLY_DISABLED_RESCORE_CONTEXT = RescoreContext.builder()
        .isRescoreExplicitlyDisabled(true)
        .build();

    @Builder.Default
    private final float oversampleFactor = DEFAULT_OVERSAMPLE_FACTOR;
    @Builder.Default
    private final boolean isRescoreExplicitlyDisabled = false;

    /**
     *
     * @return default RescoreContext
     */
    public static RescoreContext getDefault() {
        return RescoreContext.builder().build();
    }

    /**
     * Gets the number of results to return for the first pass of rescoring.
     *
     * @param finalK The final number of results to return for the entire shard
     * @return The number of results to return for the first pass of rescoring
     */
    public int getFirstPassK(int finalK) {
        return Math.min(MAX_FIRST_PASS_RESULTS, Math.max(MIN_FIRST_PASS_RESULTS, (int) Math.ceil(finalK * oversampleFactor)));
    }

    /**
     * Utility method to determine whether re-scoring should take place for a particular rescoring context.
     *
     * @param rescoreContext RescoreContext to check
     * @return true if re-scoring should take place, false otherwise
     */
    public static boolean shouldRescore(RescoreContext rescoreContext) {
        return rescoreContext != null && rescoreContext.isRescoreExplicitlyDisabled == false;
    }
}
