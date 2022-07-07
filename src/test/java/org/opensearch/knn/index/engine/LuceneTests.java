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

package org.opensearch.knn.index.engine;

import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.KNNMethod;
import org.opensearch.knn.index.KNNMethodContext;
import org.opensearch.knn.index.SpaceType;

import java.io.IOException;

import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.LUCENE_NAME;
import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.NAME;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;

public class LuceneTests extends KNNTestCase {

    public void testGetMethod() {
        expectThrows(IllegalArgumentException.class, () -> Lucene.INSTANCE.getMethod("invalid"));

        KNNMethod hnswMethod = Lucene.INSTANCE.getMethod(METHOD_HNSW);
        assertEquals(METHOD_HNSW, hnswMethod.getMethodComponent().getName());
        assertTrue(hnswMethod.getMethodComponent().getParameters().containsKey(METHOD_PARAMETER_M));
        assertTrue(hnswMethod.getMethodComponent().getParameters().containsKey(METHOD_PARAMETER_EF_CONSTRUCTION));
        assertTrue(hnswMethod.containsSpace(SpaceType.L2));
        assertTrue(hnswMethod.containsSpace(SpaceType.COSINESIMIL));
    }

    public void testValidateMethod() throws IOException {

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(NAME, METHOD_HNSW)
            .field(KNN_ENGINE, LUCENE_NAME)
            .field(METHOD_PARAMETER_SPACE_TYPE, SpaceType.L2.getValue())
            .startObject(PARAMETERS)
            .field(METHOD_PARAMETER_M, 16)
            .field(METHOD_PARAMETER_EF_CONSTRUCTION, 512)
            .endObject()
            .endObject();

        KNNMethodContext knnMethodContext = KNNMethodContext.parse(xContentBuilderToMap(xContentBuilder));

        assertNull(Lucene.INSTANCE.validateMethod(knnMethodContext));
    }

    public void testIsTrainingRequired() {
        assertFalse(Lucene.INSTANCE.isTrainingRequired(null));
    }
}
