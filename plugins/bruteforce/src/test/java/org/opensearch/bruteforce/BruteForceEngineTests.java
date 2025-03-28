/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.opensearch.test.OpenSearchTestCase;

public class BruteForceEngineTests extends OpenSearchTestCase {
    public void testSayHello() {
        BruteForceEngine engine = new BruteForceEngine();
        assertEquals("Hello from BruteForceEngine!", engine.sayHello());
    }
}
