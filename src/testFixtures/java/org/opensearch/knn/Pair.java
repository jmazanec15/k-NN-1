/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn;

import lombok.Getter;

@Getter
public class Pair<T, V> {
    private final T first;
    private final V second;

    public Pair(T first, V second) {
        this.first = first;
        this.second = second;
    }
}
