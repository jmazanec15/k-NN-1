/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.index;

import lombok.SneakyThrows;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.index.query.KNNQueryBuilder;

public class CacheExpiraryIT extends KNNRestTestCase {

    /**
     * Sets time based eviction and confirms that nothings in the cache after said time
     */
    @SneakyThrows
    public void testExpiredEntriesGetRemoved() {
        // Set circuit breaker limit to 1 KB
        updateClusterSettings("knn.cache.item.expiry.enabled", true);
        updateClusterSettings("knn.cache.item.expiry.minutes", "1m");
        addDataToCache();
        assertCacheNotEmpty();
        int evictionCountOriginal = getTotalEvictionCount();
        logger.info("Eviction Count Original", evictionCountOriginal);
        // Sleep 3 minutes to allow cache entries to expire and chron job to complete
        Thread.sleep(65 * 2 * 1000); // seconds
        assertEvictionCountIncremented(evictionCountOriginal);
    }

    @SneakyThrows
    private void addDataToCache() {
        // Create index with 1 primary and numNodes-1 replicas so that the data will be on every node in the cluster
        int numNodes = Integer.parseInt(System.getProperty("cluster.number_of_nodes", "1"));
        Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", numNodes - 1)
                .put("index.knn", true)
                .build();

        String indexName = INDEX_NAME + "1";
        createKnnIndex(indexName, settings, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector = { 1.3f, 2.2f };
        int docsInIndex = 100;
        for (int i = 0; i < docsInIndex; i++) {
            addKnnDoc(indexName, Integer.toString(i), FIELD_NAME, vector);
        }
        refreshIndex(indexName);
        forceMergeKnnIndex(indexName);
        refreshIndex(indexName);


        // Execute search on both indices - will cause eviction
        float[] qvector = { 1.9f, 2.4f };
        int k = 10;

        // Ensure that each shard is searched over so that each Lucene segment gets loaded into memory
        for (int i = 0; i < 15; i++) {
            searchKNNIndex(indexName, new KNNQueryBuilder(FIELD_NAME, qvector, k), k);
        }
    }

    @SneakyThrows
    private void assertEvictionCountIncremented(int initial) {
        int evictionCountNow = getTotalEvictionCount();
        logger.info("Eviction Count Original", evictionCountNow);
        assertEquals(initial + 1, evictionCountNow);
    }

    @SneakyThrows
    private void assertCacheNotEmpty() {
        assertNotEquals(0, getTotalGraphsInCache());
    }
}
