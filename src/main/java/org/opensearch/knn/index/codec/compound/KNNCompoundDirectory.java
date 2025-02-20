/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.compound;

import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.knn.index.engine.KNNEngine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * KNNCompoundDirectory is a wrapper class for CompoundDirectory that allows us to
 * open legacy, knn files that were created on or before OpenSearch 2.19. After this version, we no longer need the
 * custom open input because we properly use the IndexOutput abstractions.
 */
public class KNNCompoundDirectory extends CompoundDirectory {

    private final CompoundDirectory delegate;
    private final Directory dir;

    public KNNCompoundDirectory(CompoundDirectory delegate, Directory dir) {
        this.delegate = delegate;
        this.dir = dir;
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public String[] listAll() throws IOException {
        return delegate.listAll();
    }

    @Override
    public long fileLength(String name) throws IOException {
        return delegate.fileLength(name);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (isLegacy(name)) {
            return dir.openInput(name, context);
        }
        return delegate.openInput(name, context);
    }

    private boolean isLegacy(String name) throws IOException {
        return KNNEngine.getEnginesThatCreateCustomSegmentFiles().stream().anyMatch(engine -> name.endsWith(engine.getExtension()))
                && Arrays.asList(dir.listAll()).contains(name);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return delegate.getPendingDeletions();
    }
}
