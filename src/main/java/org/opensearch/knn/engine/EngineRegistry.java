/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine;

import org.opensearch.knn.engine.faiss.FaissEngine;
import org.opensearch.knn.engine.lucene.LuceneEngine;
import org.opensearch.knn.engine.nmslib.NmslibEngine;

import java.util.HashMap;
import java.util.Map;

public final class EngineRegistry {
    private final Map<String, Engine> engines;

    public EngineRegistry() {
        this.engines = new HashMap<>();
        addDefaultEngines();
    }

    private void addDefaultEngines() {
        Engine faissEngine = new FaissEngine();
        engines.put(faissEngine.getName(), faissEngine);
        Engine nmslibEngine = new NmslibEngine();
        engines.put(nmslibEngine.getName(), nmslibEngine);
        Engine luceneEngine = new LuceneEngine();
        engines.put(luceneEngine.getName(), luceneEngine);
    }

    public void register(Engine engine) {
        if (engines.containsKey(engine.getName())) {
            throw new IllegalArgumentException(
                "Engine " + engine.getName() + " has already been registered. Can only register one engine at a time."
            );
        }
        engines.put(engine.getName(), engine);
    }

    public EnginesService createEngineService() {
        return new EnginesService(engines);
    }
}
