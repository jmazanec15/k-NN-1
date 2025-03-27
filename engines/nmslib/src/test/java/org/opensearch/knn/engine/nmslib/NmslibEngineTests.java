/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine.nmslib;

import org.opensearch.knn.engine.Engine;
import org.opensearch.test.OpenSearchTestCase;

public class NmslibEngineTests extends OpenSearchTestCase {
    public void testSayHello() {
        Engine engine = new NmslibEngine();
        engine.sayHello();
    }
}
