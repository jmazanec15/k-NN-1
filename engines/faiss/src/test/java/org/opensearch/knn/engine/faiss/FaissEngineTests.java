/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine.faiss;

import org.opensearch.knn.engine.Engine;
import org.opensearch.test.OpenSearchTestCase;

public class FaissEngineTests extends OpenSearchTestCase {
    public void testSayHello() {
        Engine engine = new FaissEngine();
        engine.sayHello();
    }
}
