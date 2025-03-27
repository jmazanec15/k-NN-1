/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine.lucene;

import org.opensearch.knn.engine.Engine;
import org.opensearch.test.OpenSearchTestCase;

public class LuceneEngineTests extends OpenSearchTestCase {
    public void testSayHello() {
        Engine engine = new LuceneEngine();
        engine.sayHello();
    }
}
