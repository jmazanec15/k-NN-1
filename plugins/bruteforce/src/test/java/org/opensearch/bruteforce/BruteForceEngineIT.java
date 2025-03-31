/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.TestUtils;
import org.opensearch.knn.index.SpaceType;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.TYPE;
import static org.opensearch.knn.common.KNNConstants.TYPE_KNN_VECTOR;
import static org.opensearch.knn.index.KNNSettings.INDEX_KNN_ADVANCED_APPROXIMATE_THRESHOLD;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;

public class BruteForceEngineIT extends KNNRestTestCase {

    private static final String PROPERTIES_FIELD = "properties";
    private final static String TEST_INDEX_PREFIX_NAME = "test_index";
    private final static String TEST_FIELD_NAME = "test_field";
    private final static int TEST_DIMENSION = 32;
    private final static int DOC_COUNT = 1100;
    private final static int QUERY_COUNT = 100;
    private final static int TEST_K = 100;
    private final static double PERFECT_RECALL = 1.0;
    private final static int SHARD_COUNT = 1;
    private final static int REPLICA_COUNT = 0;
    private final static int MAX_SEGMENT_COUNT = 10;

    // Setup ground truth for all tests once
    private final static float[][] INDEX_VECTORS = TestUtils.getIndexVectors(DOC_COUNT, TEST_DIMENSION, true);
    private final static float[][] QUERY_VECTORS = TestUtils.getQueryVectors(QUERY_COUNT, TEST_DIMENSION, DOC_COUNT, true);
    private final static Map<SpaceType, List<Set<String>>> GROUND_TRUTH = Map.of(
        SpaceType.L2,
        TestUtils.computeGroundTruthValues(INDEX_VECTORS, QUERY_VECTORS, SpaceType.L2, TEST_K),
        SpaceType.COSINESIMIL,
        TestUtils.computeGroundTruthValues(INDEX_VECTORS, QUERY_VECTORS, SpaceType.COSINESIMIL, TEST_K),
        SpaceType.INNER_PRODUCT,
        TestUtils.computeGroundTruthValues(INDEX_VECTORS, QUERY_VECTORS, SpaceType.INNER_PRODUCT, TEST_K)
    );

    public void testBruteForceEngine() throws IOException, ParseException {
        Request request = new Request("GET", "/_cat/plugins");
        request.addParameter("v", "true");
        Response response = client().performRequest(request);
        logger.info("Response: {}", EntityUtils.toString(response.getEntity()));
    }

    public void testRecall() throws Exception {
        List<SpaceType> spaceTypes = List.of(SpaceType.L2);
        for (SpaceType spaceType : spaceTypes) {
            String indexName = createIndexName("bruteforce", spaceType);
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(PROPERTIES_FIELD)
                .startObject(TEST_FIELD_NAME)
                .field(TYPE, TYPE_KNN_VECTOR)
                .field(DIMENSION, TEST_DIMENSION)
                .field(KNN_ENGINE, "bruteforce")
                .endObject()
                .endObject()
                .endObject();
            createIndexAndIngestDocs(indexName, TEST_FIELD_NAME, getSettings(), builder.toString());
            assertRecall(indexName, spaceType, 0.99f);
        }
    }

    private void assertRecall(String testIndexName, SpaceType spaceType, float acceptableRecallFromPerfect) throws Exception {
        List<List<String>> searchResults = bulkSearch(testIndexName, TEST_FIELD_NAME, QUERY_VECTORS, TEST_K);
        double recallValue = TestUtils.calculateRecallValue(searchResults, GROUND_TRUTH.get(spaceType), TEST_K);
        logger.info("Recall value = {}", recallValue);
        assertEquals(PERFECT_RECALL, recallValue, acceptableRecallFromPerfect);
    }

    private String createIndexName(String knnEngine, SpaceType spaceType) {
        return String.format("%s_%s_%s", TEST_INDEX_PREFIX_NAME, knnEngine, spaceType.getValue());
    }

    private void createIndexAndIngestDocs(String indexName, String fieldName, Settings settings, String mapping) throws Exception {
        createKnnIndex(indexName, settings, mapping);
        bulkAddKnnDocs(indexName, fieldName, INDEX_VECTORS, DOC_COUNT);
        forceMergeKnnIndex(indexName, MAX_SEGMENT_COUNT);
    }

    private Settings getSettings() {
        return Settings.builder()
            .put("number_of_shards", SHARD_COUNT)
            .put("number_of_replicas", REPLICA_COUNT)
            .put("index.use_compound_file", false)
            .put("index.knn", true)
            .put(INDEX_KNN_ADVANCED_APPROXIMATE_THRESHOLD, 0)
            .build();
    }
}
