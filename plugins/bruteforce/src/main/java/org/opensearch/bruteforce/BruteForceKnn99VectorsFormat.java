/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class BruteForceKnn99VectorsFormat extends KnnVectorsFormat {

    public static final String NAME = "bruteforce99";
    private final FlatVectorsFormat delegate;

    public BruteForceKnn99VectorsFormat() {
        this(NAME);
    }

    protected BruteForceKnn99VectorsFormat(String name) {
        super(name);
        this.delegate = new Lucene99FlatVectorsFormat(new DefaultFlatVectorScorer());
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState segmentWriteState) throws IOException {
        return delegate.fieldsWriter(segmentWriteState);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState segmentReadState) throws IOException {
        return new BruteForceKnn99VectorsReader(delegate.fieldsReader(segmentReadState));
    }

    @Override
    public int getMaxDimensions(String field) {
        return delegate.getMaxDimensions(field);
    }
}
