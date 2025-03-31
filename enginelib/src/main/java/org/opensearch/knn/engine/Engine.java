package org.opensearch.knn.engine;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.search.Query;

public interface Engine {

    String getName();

    String sayHello();

    default KnnVectorsFormat getFormat() {
        return null;
    }

    default QueryFactory createQueryFactory() {
        return null;
    }

    interface QueryFactory {
        Query createKNNQuery(String field, float[] vector, int k);
    }
}
