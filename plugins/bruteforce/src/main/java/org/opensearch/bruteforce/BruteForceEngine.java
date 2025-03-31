/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.opensearch.knn.engine.Engine;

public class BruteForceEngine implements Engine {
    @Override
    public String getName() {
        return "bruteforce";
    }

    @Override
    public String sayHello() {
        return "Hello from BruteForceEngine!";
    }

    @Override
    public KnnVectorsFormat getFormat() {
        return new BruteForceKnn99VectorsFormat();
    }

    @Override
    public QueryFactory createQueryFactory() {
        return KnnFloatVectorQuery::new;
    }
}
