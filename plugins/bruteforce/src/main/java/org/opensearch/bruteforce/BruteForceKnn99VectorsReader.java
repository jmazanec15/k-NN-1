/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class BruteForceKnn99VectorsReader extends KnnVectorsReader {

    private final FlatVectorsReader delegate;

    public BruteForceKnn99VectorsReader(FlatVectorsReader delegate) {
        this.delegate = delegate;
    }

    @Override
    public void checkIntegrity() throws IOException {
        this.delegate.checkIntegrity();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String s) throws IOException {
        return this.delegate.getFloatVectorValues(s);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String s) throws IOException {
        return this.delegate.getByteVectorValues(s);
    }

    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        FloatVectorValues floatVectorValues = this.delegate.getFloatVectorValues(field);
        KnnVectorValues.DocIndexIterator docIndexIterator = floatVectorValues.iterator();
        RandomVectorScorer randomVectorScorer = delegate.getFlatVectorScorer()
            .getRandomVectorScorer(VectorSimilarityFunction.EUCLIDEAN, floatVectorValues, target);
        while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
            knnCollector.collect(docIndexIterator.docID(), randomVectorScorer.score(docIndexIterator.index()));
        }
    }

    @Override
    public void search(String s, byte[] bytes, KnnCollector knnCollector, Bits bits) throws IOException {

    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
