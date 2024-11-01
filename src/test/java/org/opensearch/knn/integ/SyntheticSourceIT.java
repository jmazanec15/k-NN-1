/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.integ;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.KNNResult;

import java.util.List;

import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.TYPE;
import static org.opensearch.knn.common.KNNConstants.TYPE_KNN_VECTOR;

/**
 * Integration tests for synthetic source feature for vector fields. Currently, with synthetic source, there are
 * a few gaps in functionality.
 * <p>
 *      What works
 *          1. Flat mapping no missing vectors
 *          2. Reindex
 *          3. Update
 *          4. Update by query
 *          5. Single level nested fields (and above functionality)
 *          6. Multiple fields per mapping
 *          7. Missing vectors for flat mapping
 * </p>
 * <p>
 *      What doesnt work:
 *          1. Deleted docs
 *          2. Deeper levels of nested fields
 *          3. Missing fields in nested fields
 * </p>
 * <p>
 *     This set of tests showcases the functionality.
 * </p>
 */
public class SyntheticSourceIT extends KNNRestTestCase {

    private final static String NESTED_NAME = "test_nested";
    private final static String FIELD_NAME = "test_vector";
    private final int TEST_DIMENSION = 768;
    private final int DOCS = 300;
    private final int SIZE = 5;

    private final String INDEX_NAME_1 = "test_index_1";
    private final String INDEX_NAME_2 = "test_index_2";

    @SneakyThrows
    public void testSynthetic_nonNestedAlldocs() {
        createKnnIndex(INDEX_NAME_1, createVectorNonNestedMappings(TEST_DIMENSION));
        createKnnIndex(INDEX_NAME_1 + "cp", createVectorNonNestedMappings(TEST_DIMENSION));
        bulkIngestRandomVectors(INDEX_NAME_1, FIELD_NAME, DOCS, TEST_DIMENSION);
        refreshIndex(INDEX_NAME_1);
        Response response = searchKNNIndex(
                INDEX_NAME_1,
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("knn")
                .startObject(FIELD_NAME)
                .field("vector", randomFloatVector(TEST_DIMENSION))
                .field("k", SIZE)
                .endObject()
                .endObject()
                .endObject()
                .endObject(),
            SIZE
        );
        //TODO add a better check here
        reindex(INDEX_NAME_1, INDEX_NAME_1 + "cp");
        refreshAllIndices();

        // Excluded
        int indexSizeInBytesExcluded = indexSizeInBytes(INDEX_NAME_1);
        logger.info("[EXCLUDED] Search Response: " + EntityUtils.toString(response.getEntity()));
        logger.info("[EXCLUDED] IndexStore Stats: " + indexSizeInBytesExcluded);

        List<KNNResult> resultsExcluded = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
    }

    @SneakyThrows
    public void testSynthetic_nestedAlldocs() {
        createKnnIndex(INDEX_NAME_2, createVectorNestedMappings(TEST_DIMENSION));
        bulkIngestRandomVectors(INDEX_NAME_1, FIELD_NAME, DOCS, TEST_DIMENSION);
        addKnnDocWithNestedField();
    }


    @SneakyThrows
    private String createVectorNonNestedMappings(final int dimension) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD)
            .startObject(FIELD_NAME)
            .field(TYPE, TYPE_KNN_VECTOR)
            .field(DIMENSION, dimension)
            .endObject()
            .endObject()
            .endObject();

        return builder.toString();
    }

    @SneakyThrows
    private String createVectorNestedMappings(final int dimension) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(PROPERTIES_FIELD)
                .startObject(NESTED_NAME)
                .field(TYPE, "nested")
                .startObject(PROPERTIES_FIELD)
                .startObject(FIELD_NAME)
                .field(TYPE, TYPE_KNN_VECTOR)
                .field(DIMENSION, dimension)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        return builder.toString();
    }
}
