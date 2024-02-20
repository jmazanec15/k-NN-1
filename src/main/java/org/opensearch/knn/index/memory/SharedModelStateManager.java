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

import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.knn.jni.JNIService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class manages allocations that can be shared between native indices. No locking is required.
 * Once a caller obtain an instance of a {@link SharedModelInfo}, it is guaranteed to be valid until it
 * is returned. {@link SharedModelInfo} are reference counted. Once the reference count goes to 0, it will be
 * freed.
 */
class SharedModelStateManager {
    private final ConcurrentHashMap<String, SharedModelInfoEntry> sharedModelInfoCache;
    private final ReadWriteLock readWriteLock;

    private static SharedModelStateManager INSTANCE;

    public static synchronized SharedModelStateManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SharedModelStateManager();
        }
        return INSTANCE;
    }

    /**
     * Constructor
     */
    public SharedModelStateManager() {
        this.sharedModelInfoCache = new ConcurrentHashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    /**
     * Return a {@link SharedModelContext} associated with the key. If no value exists, it will attempt to load it.
     * Once returned, the {@link SharedModelContext} will be valid until
     * {@link SharedModelStateManager#release(SharedModelContext)} is called. Caller must ensure that this is called
     * after it is done using it.
     *
     * On load, it will load the backing index as well as the shared model state. This is information is returned in the
     * context because the caller will not need to call {@link JNIService#loadIndex(String, Map, String, long)} if it
     * already been loaded.
     *
     * @param indexEntryContext indexEntryContext in which to load the sharedmodelcontext.
     * @param callerUUID unique identifier for caller. Will determine if the caller loaded the index.
     * @return ShareModelContext
     */
    public SharedModelContext get(NativeMemoryEntryContext.IndexEntryContext indexEntryContext, String callerUUID) {
        this.readWriteLock.readLock().lock();
        SharedModelInfoEntry entry = sharedModelInfoCache.computeIfAbsent(
            indexEntryContext.getModelId(),
            m -> load(indexEntryContext.getKey(), indexEntryContext.getParameters(), callerUUID)
        );
        entry.incRef();
        this.readWriteLock.readLock().unlock();
        return entry.getSharedModelInfoContext();
    }

    /**
     * Indicate that the {@link SharedModelContext} is no longer being used. If nothing else is using it, it will be
     * removed from the cache and evicted
     * @param sharedModelContext to return to the system.
     */
    public void release(SharedModelContext sharedModelContext) {
        this.readWriteLock.writeLock().lock();
        long refCount = 0;
        SharedModelInfoEntry sharedModelInfoEntry = null;
        String key = null;
        for (Map.Entry<String, SharedModelInfoEntry> mapEntry : this.sharedModelInfoCache.entrySet()) {
            if (mapEntry.getValue().getSharedModelInfoContext() == sharedModelContext) {
                refCount = mapEntry.getValue().decRef();
                key = mapEntry.getKey();
                sharedModelInfoEntry = mapEntry.getValue();
                break;
            }
        }

        if (key != null && sharedModelInfoEntry != null && refCount <= 0) {
            unload(key, sharedModelInfoEntry);
        }

        this.readWriteLock.writeLock().unlock();
    }

    private SharedModelInfoEntry load(String indexPath, Map<String, Object> parameters, String callerUUID) {
        KNNEngine knnEngine = KNNEngine.getEngineNameFromPath(indexPath);
        SharedModelInfo sharedModelInfo = JNIService.loadIndexAndSharedModelInfo(indexPath, parameters, knnEngine.getName());
        return new SharedModelInfoEntry(new SharedModelContext(sharedModelInfo, callerUUID));
    }

    private void unload(String key, SharedModelInfoEntry sharedModelInfoEntry) {
        this.sharedModelInfoCache.remove(key);
        JNIService.freeSharedMemory(
            sharedModelInfoEntry.getSharedModelInfoContext().getSharedModelInfo().getIndexAddress(),
            KNNEngine.FAISS.getName()
        );
    }

    /**
     * Entry class in cache. It is assumed that thread safety will be taken care of by caller.
     */
    private static final class SharedModelInfoEntry {
        private final SharedModelContext sharedModelInfoContext;
        private long referenceCount;

        private SharedModelInfoEntry(SharedModelContext sharedModelInfoContext) {
            this.sharedModelInfoContext = sharedModelInfoContext;
            this.referenceCount = 0;
        }

        private long incRef() {
            return ++referenceCount;
        }

        private long decRef() {
            return --referenceCount;
        }

        private SharedModelContext getSharedModelInfoContext() {
            return sharedModelInfoContext;
        }
    }
}
