/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine.lucene;

import org.opensearch.knn.engine.Engine;

public class LuceneEngine implements Engine {
    @Override
    public void sayHello() {
        System.out.println("Hello from Lucene Engine");
    }
}
