/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine.faiss;

import org.opensearch.knn.engine.Engine;

public class FaissEngine implements Engine {

    @Override
    public String getName() {
        return "faiss";
    }

    @Override
    public String sayHello() {
        return "Hello from Faiss Engine";
    }
}
