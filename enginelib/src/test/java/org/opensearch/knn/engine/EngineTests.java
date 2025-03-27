/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine;

import org.opensearch.test.OpenSearchTestCase;

public class EngineTests extends OpenSearchTestCase {

    public void testSayHello() {
        Engine engine = new Engine() {
            @Override
            public void sayHello() {
                System.out.println("Hello from EngineTests");
            }
        };
        engine.sayHello();
    }
}
