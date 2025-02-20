/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.compound;

import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * Custom compound format to handle knn files. This format is only needed for legacy purposes. After version 2.19,
 * we fixed how we write the custom files so that they can properly be used with compound directories. This can be
 * removed in 4.0.
 */
public class KNNCompoundFormat extends CompoundFormat {

    private final CompoundFormat delegate;

    /**
     * Constructor that takes a delegate to handle non-overridden methods
     *
     * @param delegate CompoundFormat that will handle non-overridden methods
     */
    public KNNCompoundFormat(CompoundFormat delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
        return new KNNCompoundDirectory(delegate.getCompoundReader(dir, si), dir);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        delegate.write(dir, si, context);
    }
}
