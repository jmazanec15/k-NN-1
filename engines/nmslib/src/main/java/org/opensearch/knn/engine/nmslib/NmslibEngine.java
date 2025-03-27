/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine.nmslib;

import org.opensearch.knn.engine.Engine;

public class NmslibEngine implements Engine {
    @Override
    public void sayHello() {
        System.out.println("Hello from Nmslib Engine");
    }
}
