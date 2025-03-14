/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import lombok.extern.log4j.Log4j2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Utility class for manipulating the source map
 */
@Log4j2
public class DerivedSourceMapHelper {

    public static Map<String, Object> transform(Map<String, Object> source, List<String> paths, List<Function<Object, Object>> transformers) {
        Map<String, Object> copy = new HashMap<>(source);
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            Function<Object, Object> transformer = transformers.get(i);
            if (path == null || path.isEmpty()) {
                continue;
            }
            String[] pathElements = path.split("\\.");
            transformRecursive(copy, pathElements, 0, transformer);
        }
        return copy;
    }

    private static void transformRecursive(Object current, String[] pathElements, int index, Function<Object, Object> transformer) {
        if (index == pathElements.length) {
            return;
        }

        if (current instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) current;
            // Handle case where the remaining path elements form a single key
            String remainingPath = String.join(".", Arrays.copyOfRange(pathElements, index, pathElements.length));
            if (map.containsKey(remainingPath)) {
                map.compute(remainingPath, (k, value) -> transformer.apply(value));
                return;
            }

            String key = pathElements[index];
            Object value = map.get(key);
            if (value != null) {
                transformRecursive(value, pathElements, index + 1, transformer);
            }
        } else if (current instanceof List) {
            // Handle arrays by recursively transforming each element
            List<Object> list = (List<Object>) current;
            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                if (item instanceof Map) {
                    transformRecursive(item, pathElements, index, transformer);
                }
            }
        }
    }
}
