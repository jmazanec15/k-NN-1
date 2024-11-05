/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.integ;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.index.KNNSettings;

import java.util.Locale;
import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.TYPE;
import static org.opensearch.knn.common.KNNConstants.TYPE_KNN_VECTOR;

/**
 * Integration tests for synthetic source feature for vector fields. Currently, with synthetic source, there are
 * a few gaps in functionality.
 *     //TODO: Dimensions:
 *     // 1. Data type
 *     // 2. Dimension
 *     // 3. Nested level
 *     // 4. Vectors per field
 *     // 5. Other fields
 *     // 6. Minimum number of values
 */
public class SyntheticSourceIT extends KNNRestTestCase {

    private final static String NESTED_NAME = "test_nested";
    private final static String FIELD_NAME = "test_vector";
    private final int TEST_DIMENSION = 128;
    private final int DOCS = 50;

    private static final Settings SYN_ENABLED_SETTINGS = Settings.builder()
        .put("number_of_shards", 1)
        .put("number_of_replicas", 0)
        .put("index.knn", true)
        .put(KNNSettings.KNN_SYNTHETIC_SOURCE_ENABLED, true)
        .build();
    private static final Settings SYN_DISABLED_SETTINGS = Settings.builder()
        .put("number_of_shards", 1)
        .put("number_of_replicas", 0)
        .put("index.knn", true)
        .put(KNNSettings.KNN_SYNTHETIC_SOURCE_ENABLED, false)
        .build();

    @SneakyThrows
    public void testFlatBaseCase() {
        String indexNameSynSourceEnabled = ("enabled-" + getTestName() + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String indexNameSynSourceDisabled = ("disabled-" + getTestName() + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        prepareFlatIndex(indexNameSynSourceEnabled, SYN_ENABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        prepareFlatIndex(indexNameSynSourceDisabled, SYN_DISABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        assertDocsMatch(DOCS, indexNameSynSourceEnabled, indexNameSynSourceDisabled);
        forceMergeKnnIndex(indexNameSynSourceEnabled, 10);
        forceMergeKnnIndex(indexNameSynSourceDisabled, 10);
        refreshAllIndices();
        assertIndexBigger(indexNameSynSourceDisabled, indexNameSynSourceEnabled);
        assertDocsMatch(DOCS, indexNameSynSourceEnabled, indexNameSynSourceDisabled);
        refreshAllIndices();
        forceMergeKnnIndex(indexNameSynSourceEnabled, 1);
        forceMergeKnnIndex(indexNameSynSourceDisabled, 1);
        refreshAllIndices();
        assertIndexBigger(indexNameSynSourceDisabled, indexNameSynSourceEnabled);
        assertDocsMatch(DOCS, indexNameSynSourceEnabled, indexNameSynSourceDisabled);
    }

    @SneakyThrows
    public void testFlatReindex() {
        String originalIndexNameSynSourceEnabled = ("original-enable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String originalIndexNameSynSourceDisabled = ("original-disable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String reindexFromEnabledToEnabledIndexName = ("e2e-" + getTestName() + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String reindexFromEnabledToDisabledIndexName = ("e2d-" + getTestName() + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String reindexFromDisabledToEnabledIndexName = ("d2e-" + getTestName() + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String reindexFromDisabledToDisabledIndexName = ("d2d-" + getTestName() + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);

        prepareFlatIndex(originalIndexNameSynSourceEnabled, SYN_ENABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        prepareFlatIndex(originalIndexNameSynSourceDisabled, SYN_DISABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        createKnnIndex(reindexFromEnabledToEnabledIndexName, SYN_ENABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        createKnnIndex(reindexFromEnabledToDisabledIndexName, SYN_DISABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        createKnnIndex(reindexFromDisabledToEnabledIndexName, SYN_ENABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        createKnnIndex(reindexFromDisabledToDisabledIndexName, SYN_DISABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));

        refreshAllIndices();
        reindex(originalIndexNameSynSourceEnabled, reindexFromEnabledToEnabledIndexName);
        reindex(originalIndexNameSynSourceEnabled, reindexFromEnabledToDisabledIndexName);
        reindex(originalIndexNameSynSourceDisabled, reindexFromDisabledToEnabledIndexName);
        reindex(originalIndexNameSynSourceDisabled, reindexFromDisabledToDisabledIndexName);
        refreshAllIndices();

        assertIndexBigger(originalIndexNameSynSourceDisabled, reindexFromEnabledToEnabledIndexName);
        assertIndexBigger(originalIndexNameSynSourceDisabled, reindexFromDisabledToEnabledIndexName);
        assertIndexBigger(reindexFromEnabledToDisabledIndexName, originalIndexNameSynSourceEnabled);
        assertIndexBigger(reindexFromDisabledToDisabledIndexName, originalIndexNameSynSourceEnabled);

        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, reindexFromEnabledToEnabledIndexName);
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, reindexFromDisabledToEnabledIndexName);
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, reindexFromEnabledToDisabledIndexName);
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, reindexFromDisabledToDisabledIndexName);
    }

    @SneakyThrows
    public void testFlatDeletesAndUpdates() {
        String originalIndexNameSynSourceEnabled = ("original-enable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String originalIndexNameSynSourceDisabled = ("original-disable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        prepareFlatIndex(originalIndexNameSynSourceEnabled, SYN_ENABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));
        prepareFlatIndex(originalIndexNameSynSourceDisabled, SYN_DISABLED_SETTINGS, createVectorNonNestedMappings(TEST_DIMENSION));

        int docWithVectorUpdate = DOCS - 4;
        int docWithVectorRemoval = 1;
        int docWithVectorUpdateFromAPI = 2;
        int docWithUpdateByQuery = 7;
        int docToDelete = 8;
        int docToDeleteByQuery = 11;

        float[] updateVector = randomFloatVector(TEST_DIMENSION);
        updateKnnDoc(
            originalIndexNameSynSourceEnabled,
            String.valueOf(docWithVectorUpdate),
            FIELD_NAME,
            Floats.asList(updateVector).toArray()
        );
        updateKnnDoc(
            originalIndexNameSynSourceDisabled,
            String.valueOf(docWithVectorUpdate),
            FIELD_NAME,
            Floats.asList(updateVector).toArray()
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);

        setDocToEmpty(originalIndexNameSynSourceEnabled, String.valueOf(docWithVectorRemoval));
        setDocToEmpty(originalIndexNameSynSourceDisabled, String.valueOf(docWithVectorRemoval));
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);

        updateKnnDocWithUpdateAPI(
            originalIndexNameSynSourceEnabled,
            String.valueOf(docWithVectorUpdateFromAPI),
            FIELD_NAME,
            Floats.asList(updateVector).toArray()
        );
        updateKnnDocWithUpdateAPI(
            originalIndexNameSynSourceDisabled,
            String.valueOf(docWithVectorUpdateFromAPI),
            FIELD_NAME,
            Floats.asList(updateVector).toArray()
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);

        updateKnnDocByQuery(
            originalIndexNameSynSourceEnabled,
            String.valueOf(docWithUpdateByQuery),
            FIELD_NAME,
            Floats.asList(updateVector).toArray()
        );
        updateKnnDocByQuery(
            originalIndexNameSynSourceDisabled,
            String.valueOf(docWithUpdateByQuery),
            FIELD_NAME,
            Floats.asList(updateVector).toArray()
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);

        deleteKnnDoc(originalIndexNameSynSourceEnabled, String.valueOf(docToDelete));
        deleteKnnDoc(originalIndexNameSynSourceDisabled, String.valueOf(docToDelete));
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);

        deleteKnnDocByQuery(originalIndexNameSynSourceEnabled, String.valueOf(docToDeleteByQuery));
        deleteKnnDocByQuery(originalIndexNameSynSourceDisabled, String.valueOf(docToDeleteByQuery));
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
    }

    @SneakyThrows
    public void testMultiFlatFields() {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD)
            .startObject(FIELD_NAME + "1")
            .field(TYPE, TYPE_KNN_VECTOR)
            .field(DIMENSION, TEST_DIMENSION)
            .endObject()
            .startObject(FIELD_NAME + "2")
            .field(TYPE, TYPE_KNN_VECTOR)
            .field(DIMENSION, TEST_DIMENSION)
            .endObject()
            .startObject("text")
            .field(TYPE, "text")
            .endObject()
            .endObject()
            .endObject();

        String originalIndexNameSynSourceEnabled = ("original-enable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String originalIndexNameSynSourceDisabled = ("original-disable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);

        createKnnIndex(originalIndexNameSynSourceEnabled, SYN_ENABLED_SETTINGS, builder.toString());
        createKnnIndex(originalIndexNameSynSourceDisabled, SYN_DISABLED_SETTINGS, builder.toString());
        bulkIngestRandomVectorsWithSkipsAndMultFields(
            originalIndexNameSynSourceEnabled,
            FIELD_NAME + "1",
            FIELD_NAME + "2",
            "text",
            DOCS,
            TEST_DIMENSION,
            0.1f
        );
        bulkIngestRandomVectorsWithSkipsAndMultFields(
            originalIndexNameSynSourceDisabled,
            FIELD_NAME + "1",
            FIELD_NAME + "2",
            "text",
            DOCS,
            TEST_DIMENSION,
            0.1f
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceEnabled, originalIndexNameSynSourceDisabled);
        forceMergeKnnIndex(originalIndexNameSynSourceEnabled, 10);
        forceMergeKnnIndex(originalIndexNameSynSourceDisabled, 10);
        refreshAllIndices();
        assertIndexBigger(originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
        assertDocsMatch(DOCS, originalIndexNameSynSourceEnabled, originalIndexNameSynSourceDisabled);
        refreshAllIndices();
        forceMergeKnnIndex(originalIndexNameSynSourceEnabled, 1);
        forceMergeKnnIndex(originalIndexNameSynSourceDisabled, 1);
        refreshAllIndices();
        assertIndexBigger(originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
        assertDocsMatch(DOCS, originalIndexNameSynSourceEnabled, originalIndexNameSynSourceDisabled);

        int docWithVectorUpdate = DOCS - 4;
        int docWithUpdateByQuery = 7;

        float[] updateVector = randomFloatVector(TEST_DIMENSION);
        updateKnnDoc(
            originalIndexNameSynSourceEnabled,
            String.valueOf(docWithVectorUpdate),
            FIELD_NAME + "1",
            Floats.asList(updateVector).toArray()
        );
        updateKnnDoc(
            originalIndexNameSynSourceDisabled,
            String.valueOf(docWithVectorUpdate),
            FIELD_NAME + "1",
            Floats.asList(updateVector).toArray()
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);

        updateKnnDocByQuery(
            originalIndexNameSynSourceEnabled,
            String.valueOf(docWithUpdateByQuery),
            FIELD_NAME + "2",
            Floats.asList(updateVector).toArray()
        );
        updateKnnDocByQuery(
            originalIndexNameSynSourceDisabled,
            String.valueOf(docWithUpdateByQuery),
            FIELD_NAME + "3",
            Floats.asList(updateVector).toArray()
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
    }

    @SneakyThrows
    public void testNestedSingleDocBasic() {
        // For basic tests, we will have 0-5 nested documents per document
        String originalIndexNameSynSourceEnabled = ("original-enable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);
        String originalIndexNameSynSourceDisabled = ("original-disable-" + randomAlphaOfLength(6)).toLowerCase(Locale.ROOT);

        createKnnIndex(originalIndexNameSynSourceEnabled, SYN_ENABLED_SETTINGS, createVectorNestedMappings(TEST_DIMENSION));
        createKnnIndex(originalIndexNameSynSourceDisabled, SYN_DISABLED_SETTINGS, createVectorNestedMappings(TEST_DIMENSION));

        bulkIngestRandomVectorsWithSkipsAndNested(
            originalIndexNameSynSourceEnabled,
            NESTED_NAME + "." + FIELD_NAME,
            NESTED_NAME + "." + "text",
            DOCS,
            TEST_DIMENSION,
            0.1f
        );
        bulkIngestRandomVectorsWithSkipsAndNested(
            originalIndexNameSynSourceDisabled,
            NESTED_NAME + "." + FIELD_NAME,
            NESTED_NAME + "." + "text",
            DOCS,
            TEST_DIMENSION,
            0.1f
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceEnabled, originalIndexNameSynSourceDisabled);
        forceMergeKnnIndex(originalIndexNameSynSourceEnabled, 10);
        forceMergeKnnIndex(originalIndexNameSynSourceDisabled, 10);
        refreshAllIndices();
        assertIndexBigger(originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
        assertDocsMatch(DOCS, originalIndexNameSynSourceEnabled, originalIndexNameSynSourceDisabled);
        refreshAllIndices();
        forceMergeKnnIndex(originalIndexNameSynSourceEnabled, 1);
        forceMergeKnnIndex(originalIndexNameSynSourceDisabled, 1);
        refreshAllIndices();
    }

    @SneakyThrows
    public void testNestedMultiDocBasic() {
        String originalIndexNameSynSourceEnabled = ("original-enable-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT)); // "test");
                                                                                                                           // /*randomAlphaOfLength(4)).toLowerCase(Locale.ROOT)*/;
        String originalIndexNameSynSourceDisabled = ("original-disable-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT)); // + ;
                                                                                                                             // //"test");
                                                                                                                             // /*randomAlphaOfLength(4)).toLowerCase(Locale.ROOT)*/;

        createKnnIndex(originalIndexNameSynSourceEnabled, SYN_ENABLED_SETTINGS, createVectorNestedMappings(TEST_DIMENSION));
        createKnnIndex(originalIndexNameSynSourceDisabled, SYN_DISABLED_SETTINGS, createVectorNestedMappings(TEST_DIMENSION));

        bulkIngestRandomVectorsWithSkipsAndNestedMultiDoc(
            originalIndexNameSynSourceEnabled,
            NESTED_NAME + "." + FIELD_NAME,
            NESTED_NAME + "." + "text",
            DOCS,
            TEST_DIMENSION,
            0.1f,
            5
        );
        bulkIngestRandomVectorsWithSkipsAndNestedMultiDoc(
            originalIndexNameSynSourceDisabled,
            NESTED_NAME + "." + FIELD_NAME,
            NESTED_NAME + "." + "text",
            DOCS,
            TEST_DIMENSION,
            0.1f,
            5
        );
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
        forceMergeKnnIndex(originalIndexNameSynSourceEnabled, 10);
        forceMergeKnnIndex(originalIndexNameSynSourceDisabled, 10);
        refreshAllIndices();
        assertIndexBigger(originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
        refreshAllIndices();
        forceMergeKnnIndex(originalIndexNameSynSourceEnabled, 1);
        forceMergeKnnIndex(originalIndexNameSynSourceDisabled, 1);
        refreshAllIndices();
        assertDocsMatch(DOCS, originalIndexNameSynSourceDisabled, originalIndexNameSynSourceEnabled);
    }

    // public void testNestedReindex() {
    //
    // }
    //
    // public void testNestedUpdateAndDelete() {
    //
    // }
    //
    // public void testMultiNestedFields() {
    // // TODO
    // }
    //
    // public void testMixedNestedAndFlatFields() {
    // // TODO
    // }
    //
    // public void testFLSSupport() {
    // // TODO: Security only - need to figure out how to configure this one better
    // }
    //
    // public void testNullSet() {
    // // TODO: we know this breaks
    // }

    @SneakyThrows
    private void assertIndexBigger(String expectedBiggerIndex, String expectedSmallerIndex) {
        assertTrue(indexSizeInBytes(expectedSmallerIndex) < indexSizeInBytes(expectedBiggerIndex));
    }

    @SneakyThrows
    private void prepareFlatIndex(String indexName, Settings settings, String mapping) {
        createKnnIndex(indexName, settings, mapping);
        bulkIngestRandomVectorsWithSkips(indexName, FIELD_NAME, DOCS, TEST_DIMENSION, 0.1f);
        refreshAllIndices();
    }

    private void assertDocsMatch(int docCount, String index1, String index2) {
        for (int i = 0; i < docCount; i++) {
            assertDocMatches(i + 1, index1, index2);
        }
    }

    @SneakyThrows
    private void assertDocMatches(int docId, String index1, String index2) {
        Map<String, Object> response1 = getKnnDoc(index1, String.valueOf(docId));
        Map<String, Object> response2 = getKnnDoc(index2, String.valueOf(docId));
        assertEquals("Docs do not match: " + docId, response1, response2);
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
