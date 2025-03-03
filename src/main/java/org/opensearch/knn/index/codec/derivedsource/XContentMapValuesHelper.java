/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.xcontent.support.XContentMapValues.nodeMapValue;

public class XContentMapValuesHelper {

    private static final String TRANSFORMER_TRIE_LEAF_KEY = "$transformer";

    /**
     * Performs a depth first traversal of a map and applies a transformation for each field matched along the way. For
     * duplicated paths with transformers (i.e. "test.nested" and "test.nested.field"), only the transformer for
     * the shorter path is applied.
     *
     * @param source Source map to perform transformation on
     * @param transformers Map from path to transformer to apply to each path. Each transformer is a function that takes
     *                    the current value and returns a transformed value
     * @param inPlace If true, modify the source map directly; if false, create a copy
     * @return Map with transformations applied
     */
    public static Map<String, Object> transform(
        Map<String, Object> source,
        Map<String, Function<Object, Object>> transformers,
        boolean inPlace
    ) {
        return transform(transformers, inPlace).apply(source);
    }

    /**
     * Returns function that performs a depth first traversal of a map and applies a transformation for each field
     * matched along the way. For duplicated paths with transformers (i.e. "test.nested" and "test.nested.field"), only
     * the transformer for the shorter path is applied.
     *
     * @param transformers Map from path to transformer to apply to each path. Each transformer is a function that takes
     *                     the current value and returns a transformed value
     * @param inPlace If true, modify the source map directly; if false, create a copy
     * @return Function that takes a map and returns a transformed version of the map
     */
    public static Function<Map<String, Object>, Map<String, Object>> transform(
        Map<String, Function<Object, Object>> transformers,
        boolean inPlace
    ) {
        Map<String, Object> transformerTrie = buildTransformerTrie(transformers);
        return source -> {
            Deque<TransformContext> stack = new ArrayDeque<>();
            Map<String, Object> result = inPlace ? source : new HashMap<>(source);
            stack.push(new TransformContext(result, transformerTrie));

            processStack(stack, inPlace);
            return result;
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> buildTransformerTrie(Map<String, Function<Object, Object>> transformers) {
        Map<String, Object> trie = new HashMap<>();
        for (Map.Entry<String, Function<Object, Object>> entry : transformers.entrySet()) {
            String[] pathElements = entry.getKey().split("\\.");
            Map<String, Object> subTrie = trie;
            for (String pathElement : pathElements) {
                subTrie = (Map<String, Object>) subTrie.computeIfAbsent(pathElement, k -> new HashMap<>());
            }
            subTrie.put(TRANSFORMER_TRIE_LEAF_KEY, entry.getValue());
        }
        return trie;
    }

    private static void processStack(Deque<TransformContext> stack, boolean inPlace) {
        while (!stack.isEmpty()) {
            TransformContext ctx = stack.pop();
            processMap(ctx.map, ctx.trie, stack, inPlace);
        }
    }

    private static void processMap(
        Map<String, Object> currentMap,
        Map<String, Object> currentTrie,
        Deque<TransformContext> stack,
        boolean inPlace
    ) {
        for (Map.Entry<String, Object> entry : currentMap.entrySet()) {
            processEntry(entry, currentTrie, stack, inPlace);
        }
    }

    private static void processEntry(
        Map.Entry<String, Object> entry,
        Map<String, Object> currentTrie,
        Deque<TransformContext> stack,
        boolean inPlace
    ) {
        String key = entry.getKey();
        Object value = entry.getValue();

        Object subTrieObj = currentTrie.get(key);
        if (subTrieObj instanceof Map == false) {
            return;
        }
        Map<String, Object> subTrie = nodeMapValue(subTrieObj, "transform");

        // Apply transformation if available
        Function<Object, Object> transformer = (Function<Object, Object>) subTrie.get(TRANSFORMER_TRIE_LEAF_KEY);
        if (transformer != null) {
            entry.setValue(transformer.apply(value));
            return;
        }

        // Process nested structures
        if (value instanceof Map) {
            Map<String, Object> subMap = nodeMapValue(value, "transform");
            if (inPlace == false) {
                subMap = new HashMap<>(subMap);
                entry.setValue(subMap);
            }
            stack.push(new TransformContext(subMap, subTrie));
        } else if (value instanceof List<?>) {
            List<Object> subList = (List<Object>) value;
            if (inPlace == false) {
                subList = new ArrayList<>(subList);
                entry.setValue(subList);
            }
            processList(subList, subTrie, stack, inPlace);
        }
    }

    private static void processList(
        List<Object> list,
        Map<String, Object> transformerTrie,
        Deque<TransformContext> stack,
        boolean inPlace
    ) {
        for (int i = list.size() - 1; i >= 0; i--) {
            Object value = list.get(i);
            if (value instanceof Map) {
                Map<String, Object> subMap = nodeMapValue(value, "transform");
                if (inPlace == false) {
                    subMap = new HashMap<>(subMap);
                    list.set(i, subMap);
                }
                stack.push(new TransformContext(subMap, transformerTrie));
            }
        }
    }

    private static class TransformContext {
        Map<String, Object> map;
        Map<String, Object> trie;

        TransformContext(Map<String, Object> map, Map<String, Object> trie) {
            this.map = map;
            this.trie = trie;
        }
    }

}
