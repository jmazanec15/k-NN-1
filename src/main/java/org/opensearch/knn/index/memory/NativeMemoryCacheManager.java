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

package org.opensearch.knn.index.memory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.knn.common.exception.OutOfNativeMemoryException;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.plugin.stats.StatNames;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages native memory allocations made by JNI.
 */
public final class NativeMemoryCacheManager implements Closeable {

    public static String GRAPH_COUNT = "graph_count";

    private static Logger logger = LogManager.getLogger(NativeMemoryCacheManager.class);
    private static NativeMemoryCacheManager INSTANCE;

    private Cache<String, NativeMemoryAllocation> cache;
    private ExecutorService executor;
    private AtomicBoolean cacheCapacityReached;
    private long maxWeight;

    NativeMemoryCacheManager() {
        this.executor = Executors.newSingleThreadExecutor();
        this.cacheCapacityReached = new AtomicBoolean(false);
        this.maxWeight = Long.MAX_VALUE;
        initialize();
    }

    /**
     * Make sure we just have one instance of cache.
     *
     * @return NativeMemoryCacheManager instance
     */
    public static synchronized NativeMemoryCacheManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new NativeMemoryCacheManager();
        }
        return INSTANCE;
    }

    private void initialize() {
        CacheBuilder<String, NativeMemoryAllocation> cacheBuilder = CacheBuilder.newBuilder()
                .recordStats()
                .concurrencyLevel(1)
                .removalListener(this::onRemoval);

        if(KNNSettings.state().getSettingValue(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_ENABLED)) {
            maxWeight = KNNSettings.getCircuitBreakerLimit().getKb();
            cacheBuilder.maximumWeight(maxWeight).weigher((k, v) -> (int)v.getSizeInKb());
        }

        if(KNNSettings.state().getSettingValue(KNNSettings.KNN_CACHE_ITEM_EXPIRY_ENABLED)) {
            long expiryTime = ((TimeValue) KNNSettings.state()
                    .getSettingValue(KNNSettings.KNN_CACHE_ITEM_EXPIRY_TIME_MINUTES)).getMinutes();
            cacheBuilder.expireAfterAccess(expiryTime, TimeUnit.MINUTES);
        }

        cacheCapacityReached = new AtomicBoolean(false);

        cache = cacheBuilder.build();
    }

    /**
     * Evicts all entries from the cache and rebuilds.
     */
    public synchronized void rebuildCache() {
        logger.info("KNN Cache rebuilding.");
        executor.execute(() -> {
            cache.invalidateAll();
            initialize(); }
        );
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    /**
     * Getter for current cache size in Kilobytes.
     *
     * @return current size of the cache
     */
    public long getCacheSizeInKilobytes() {
        return cache.asMap().values().stream().mapToLong(NativeMemoryAllocation::getSizeInKb).sum();
    }

    /**
     * Returns the current size of an index in the cache in KiloBytes.
     *
     * @param indexName Name if index to get the weight for
     * @return Size of the index in the cache in kilobytes
     */
    public Long getIndexSizeInKilobytes(final String indexName) {
        Validate.notNull(indexName, "Index name cannot be null");
        return cache.asMap().values().stream()
                .filter(nativeMemoryAllocation -> nativeMemoryAllocation instanceof NativeMemoryAllocation.IndexAllocation)
                .filter(indexAllocation -> indexName.equals(((NativeMemoryAllocation.IndexAllocation) indexAllocation).getOpenSearchIndexName()))
                .mapToLong(NativeMemoryAllocation::getSizeInKb)
                .sum();
    }

    /**
     * Returns how full the cache is as a percentage of the total cache capacity.
     *
     * @return Percentage of the cache full
     */
    public Float getCacheSizeAsPercentage() {
        return 100 * getCacheSizeInKilobytes() / (float) KNNSettings.getCircuitBreakerLimit().getKb();
    }

    /**
     * Returns the how much space an index is taking up in the cache as a percentage of the total cache capacity.
     *
     * @param indexName name of the index
     * @return Percentage of the cache full
     */
    public Float getIndexSizeAsPercentage(final String indexName) {
        Validate.notNull(indexName, "Index name cannot be null");
        return 100 * getIndexSizeInKilobytes(indexName) / (float) KNNSettings.getCircuitBreakerLimit().getKb();
    }

    /**
     * Getter for maximum weight of the cache.
     *
     * @return maximum cache weight
     */
    public long getMaxCacheSizeInKilobytes() {
        return maxWeight;
    }

    /**
     * Get graph count for a particular index
     *
     * @param indexName name of OpenSearch index
     * @return number of graphs for a particular OpenSearch index
     */
    public int getIndexGraphCount(String indexName) {
        Validate.notNull(indexName, "Index name cannot be null");
        return Long.valueOf(cache.asMap().values().stream()
                .filter(nativeMemoryAllocation ->
                        nativeMemoryAllocation instanceof NativeMemoryAllocation.IndexAllocation)
                .filter(indexAllocation -> indexName.equals(((NativeMemoryAllocation.IndexAllocation) indexAllocation)
                        .getOpenSearchIndexName()))
                .count()).intValue();
    }

    /**
     * Getter for cache stats.
     *
     * @return cache status
     */
    public CacheStats getCacheStats() {
        return cache.stats();
    }

    /**
     * Getter for cache as a map.
     *
     * @return cache as a map
     */
    public Map<String, NativeMemoryAllocation> getCacheAsMap() {
        return cache.asMap();
    }

    /**
     * Retrieves NativeMemoryAllocation associated with the nativeMemoryEntryContext.
     *
     * @param nativeMemoryEntryContext Context from which to get NativeMemoryAllocation
     * @param isAbleToTriggerEviction Determines if getting this allocation can evict other entries
     * @return NativeMemoryAllocation associated with nativeMemoryEntryContext
     * @throws ExecutionException if there is an exception when loading from the cache
     */
    public NativeMemoryAllocation get(NativeMemoryEntryContext<?> nativeMemoryEntryContext,
                                      boolean isAbleToTriggerEviction) throws ExecutionException {
        if (!isAbleToTriggerEviction &&
                !cache.asMap().containsKey(nativeMemoryEntryContext.getKey()) &&
                maxWeight - getCacheSizeInKilobytes() - nativeMemoryEntryContext.calculateSizeInKb() <= 0
        ) {
            throw new OutOfNativeMemoryException("Failed to load \"" + nativeMemoryEntryContext.getKey() +
                    "\" into memory.");
        }

        return cache.get(nativeMemoryEntryContext.getKey(), nativeMemoryEntryContext::load);
    }

    /**
     * Invalidate entry from the cache.
     *
     * @param key Identifier of entry to invalidate
     */
    public void invalidate(String key) {
        cache.invalidate(key);
    }

    /**
     * Invalidate all entries in the cache.
     */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /**
     * Returns whether or not the capacity of the cache has been reached
     *
     * @return Boolean of whether cache limit has been reached
     */
    public Boolean isCacheCapacityReached() {
        return cacheCapacityReached.get();
    }

    /**
     * Sets cache capacity reached
     *
     * @param value Boolean value to set cache Capacity Reached to
     */
    public void setCacheCapacityReached(Boolean value) {
        cacheCapacityReached.set(value);
    }

    /**
     * Get the stats of all of the Elasticsearch indices currently loaded into the cache
     *
     * @return Map containing all of the Elasticsearch indices in the cache and their stats
     */
    public Map<String, Map<String, Object>> getIndicesCacheStats() {
        Map<String, Map<String, Object>> statValues = new HashMap<>();
        NativeMemoryAllocation.IndexAllocation indexAllocation;

        for (Map.Entry<String, NativeMemoryAllocation> entry : cache.asMap().entrySet()) {

            if (entry.getValue() instanceof NativeMemoryAllocation.IndexAllocation) {
                indexAllocation = (NativeMemoryAllocation.IndexAllocation) entry.getValue();
                String indexName = indexAllocation.getOpenSearchIndexName();

                Map<String, Object> indexMap = statValues.computeIfAbsent(indexName, name -> new HashMap<>());
                indexMap.computeIfAbsent(GRAPH_COUNT, key -> getIndexGraphCount(indexName));
                indexMap.computeIfAbsent(StatNames.GRAPH_MEMORY_USAGE.getName(), key ->
                        getIndexSizeInKilobytes(indexName));
                indexMap.computeIfAbsent(StatNames.GRAPH_MEMORY_USAGE_PERCENTAGE.getName(), key ->
                        getIndexSizeAsPercentage(indexName));
            }
        }

        return statValues;
    }


    private void onRemoval(RemovalNotification<String, NativeMemoryAllocation> removalNotification) {
        NativeMemoryAllocation nativeMemoryAllocation = removalNotification.getValue();
        executor.execute(() -> {
            nativeMemoryAllocation.writeLock();

            try {
                nativeMemoryAllocation.close();
            } finally {
                nativeMemoryAllocation.writeUnlock();
            }
        });

        if (RemovalCause.SIZE == removalNotification.getCause()) {
            KNNSettings.state().updateCircuitBreakerSettings(true);
            setCacheCapacityReached(true);
        }

        logger.debug("[KNN] Cache evicted. Key {}, Reason: {}", removalNotification.getKey(),
                removalNotification.getCause());
    }
}
