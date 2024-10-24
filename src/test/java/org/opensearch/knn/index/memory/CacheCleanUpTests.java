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
import lombok.SneakyThrows;
import org.opensearch.knn.KNNTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CacheCleanUpTests extends KNNTestCase {

    private static final int EXPIRE_TIME_IN_SECONDS = 1;

    @SneakyThrows
    public void testExpiredEntriesGetCleared() {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        CacheBuilder<String, Integer> cacheBuilder = CacheBuilder.newBuilder()
                .recordStats()
                .concurrencyLevel(1)
                .expireAfterAccess(2, TimeUnit.SECONDS)
                .removalListener(removalNotification -> {
                    inProgressLatch.countDown();
                });

        Cache<String, Integer> cache = cacheBuilder.build();
        Integer integer = cache.get("test", () -> 1);
        assertEquals(cache.size(), 1);
        Thread.sleep(2*EXPIRE_TIME_IN_SECONDS*1000);
        assertEquals(0, cache.stats().evictionCount());
        cache.cleanUp();
        assertEquals(1, cache.stats().evictionCount());
        assertEquals(0, inProgressLatch.getCount());
    }
}
