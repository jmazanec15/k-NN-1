/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.engine;

import lombok.extern.log4j.Log4j2;

import java.util.Map;

@Log4j2
public record EnginesService(Map<String, Engine> engines) {
    public Engine getEngine(String engineName) {
        if (!engines.containsKey(engineName)) {
            throw new IllegalArgumentException("Engine " + engineName + " does not exist");
        }
        return engines.get(engineName);
    }

    public void logEngines() {
        for (Engine engine : engines.values()) {
            log.info("Engine \"{}\" says \"{}\"", engine.getName(), engine.sayHello());
        }
    }
}
