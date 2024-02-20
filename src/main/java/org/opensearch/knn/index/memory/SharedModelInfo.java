/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.memory;

public class SharedModelInfo {
    private final long indexAddress;
    private final long sharedTableAddress;

    public SharedModelInfo(long sharedTableAddress, long indexAddress) {
        this.sharedTableAddress = sharedTableAddress;
        this.indexAddress = indexAddress;
    }

    public long getIndexAddress() {
        return indexAddress;
    }

    public long getSharedTableAddress() {
        return sharedTableAddress;
    }
}
