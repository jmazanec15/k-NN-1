/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.opensearch.knn.engine.Engine;

public class BruteForceEngine implements Engine {
    @Override
    public String getName() {
        return "bruteforce";
    }

    @Override
    public String sayHello() {
        return "Hello from BruteForceEngine!";
    }
}
