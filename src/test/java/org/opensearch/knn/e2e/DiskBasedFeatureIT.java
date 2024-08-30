/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.e2e;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;

import static org.opensearch.knn.common.KNNConstants.COMPRESSION_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.K;
import static org.opensearch.knn.common.KNNConstants.KNN;
import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.KNN_METHOD;
import static org.opensearch.knn.common.KNNConstants.METHOD_ENCODER_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.MODEL;
import static org.opensearch.knn.common.KNNConstants.MODE_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.NAME;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;
import static org.opensearch.knn.common.KNNConstants.QUERY;
import static org.opensearch.knn.common.KNNConstants.TYPE;
import static org.opensearch.knn.common.KNNConstants.TYPE_KNN_VECTOR;
import static org.opensearch.knn.common.KNNConstants.VECTOR;
import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;
import static org.opensearch.knn.index.query.parser.RescoreParser.RESCORE_OVERSAMPLE_PARAMETER;
import static org.opensearch.knn.index.query.parser.RescoreParser.RESCORE_PARAMETER;

@Log4j2
public class DiskBasedFeatureIT extends KNNRestTestCase {

    public static int DEFAULT_DIMENSION = 8;
    public static String DEFAULT_FIELD_NAME = "testfield";

    @SneakyThrows
    public void testValid_NoMode_flat() {
        execTestFeature(
            TestConfiguration.builder()
                .testDescription("KNN Disabled setting disabled")
                .shouldBasicSearchWork(false)
                .shouldRescoreSearchWork(false)
                .isKNNSettingEnabled(false)
                .build()
        );
    }

    @SneakyThrows
    public void testValid_NoMode_faissnoparams() {
        execTestFeature(
            TestConfiguration.builder()
                .testDescription("Faiss from method")
                .shouldBasicSearchWork(true)
                .shouldRescoreSearchWork(true)
                .isKNNSettingEnabled(true)
                .methodMappingBuilderConsumer(
                    builder -> builder
                            .field(NAME, "hnsw")
                            .field(METHOD_PARAMETER_SPACE_TYPE, "l2")
                            .field(KNN_ENGINE, "faiss")
                )
                .build()
        );
    }

    @SneakyThrows
    public void testValid_NoMode_faissANDBQ() {
        execTestFeature(
            TestConfiguration.builder()
                .testDescription("KNN Disabled setting disabled")
                .shouldBasicSearchWork(true)
                .shouldRescoreSearchWork(true)
                .isKNNSettingEnabled(true)
                .methodMappingBuilderConsumer(
                    builder -> builder.field(NAME, "hnsw")
                            .field(METHOD_PARAMETER_SPACE_TYPE, "l2")
                            .field(KNN_ENGINE, "faiss")
                            .startObject(PARAMETERS)
                            .startObject(METHOD_ENCODER_PARAMETER)
                            .field(NAME, "binary")
                            .startObject(PARAMETERS)
                            .field("bits", 2)
                            .endObject()
                            .endObject()
                            .endObject()
                )
                .build()
        );
    }

    @SneakyThrows
    public void testValid_Mode_OnDiskAndDefaults() {
        execTestFeature(
                TestConfiguration.builder()
                        .testDescription("Mode based disk")
                        .shouldBasicSearchWork(true)
                        .shouldRescoreSearchWork(true)
                        .isKNNSettingEnabled(true)
                        .mode(WorkloadModeConfig.ON_DISK.toString())
                        .build()
        );
    }

    @SneakyThrows
    public void testValid_Mode_OnDiskAndCompression16x() {
        execTestFeature(
                TestConfiguration.builder()
                        .testDescription("Mode based disk")
                        .shouldBasicSearchWork(true)
                        .shouldRescoreSearchWork(true)
                        .isKNNSettingEnabled(true)
                        .mode(WorkloadModeConfig.ON_DISK.toString())
                        .compression("x16")
                        .build()
        );
    }


    @SneakyThrows
    private void execTestFeature(TestConfiguration testConfiguration) {
        testConfiguration.setIndexName(randomAlphaOfLength(10).toLowerCase());

        log.info("Test \"{}\"", testConfiguration.getTestDescription());
        log.info("index: \"{}\"", testConfiguration.getIndexName());

        TestConfiguration trainingTestConfiguration = validateTraining(testConfiguration);

        validateCreateIndex(testConfiguration);

        validateIngestData(testConfiguration);

        validateBasicSearch(testConfiguration);

        validateRescoreSearch(testConfiguration);

        validateIndexDeletion(testConfiguration);

        if (trainingTestConfiguration != null) {
            validateIndexDeletion(testConfiguration);
            validateModelDeletion(testConfiguration);
        }
//        fail();
    }

    @SneakyThrows
    private TestConfiguration validateTraining(TestConfiguration testConfiguration) {
        if (testConfiguration.requiresTraining == false) {
            return null;
        }
        String modelId = testConfiguration.modelId;

        TestConfiguration trainingConfiguration = TestConfiguration.builder()
            .isKNNSettingEnabled(false)
            .dimension(testConfiguration.dimension)
            .vectorDataType(testConfiguration.vectorDataType)
            .indexDocumentCount(testConfiguration.trainingDataRequired)
            .shouldDelete(false)
            .indexName(randomAlphaOfLength(10).toLowerCase())
            .build();

        // Create index
        validateCreateIndex(testConfiguration);

        // Load data
        validateIngestData(testConfiguration);

        // Create training request
        createTrainingRequest(trainingConfiguration, modelId);

        // training
        return trainingConfiguration;
    }

    @SneakyThrows
    private void createTrainingRequest(TestConfiguration testConfiguration, String modelId) {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        testConfiguration.methodMappingBuilderConsumer.accept(builder);

        Response trainResponse = trainModel(
            modelId,
            testConfiguration.indexName,
            DEFAULT_FIELD_NAME,
            testConfiguration.dimension,
            builder.toString(),
            ""
        );
        assertEquals(RestStatus.OK, RestStatus.fromCode(trainResponse.getStatusLine().getStatusCode()));
        assertTrainingSucceeds(modelId, 360, 1000);
    }

    @SneakyThrows
    private void validateCreateIndex(TestConfiguration testConfiguration) {
        log.info("Mapping: {}", createVectorMappings(testConfiguration));
        log.info("Settings: {}", createSettings(testConfiguration));
        createKnnIndex(testConfiguration.getIndexName(), createSettings(testConfiguration), createVectorMappings(testConfiguration));
        log.info("Mapping: {}", getIndexMappingAsMap(testConfiguration.getIndexName()));
        log.info("Settings: {}", getIndexSettings(testConfiguration.getIndexName()));
    }

    @SneakyThrows
    private void validateIngestData(TestConfiguration testConfiguration) {
        float[][] data = new float[testConfiguration.getIndexDocumentCount()][];
        for (int i = 0; i < testConfiguration.getIndexDocumentCount(); i++) {
            float[] vector = new float[testConfiguration.getDimension()];
            for (int j = 0; j < testConfiguration.getDimension(); j++) {
                vector[j] = randomFloat();
            }
            data[i] = vector;
        }
        bulkAddKnnDocs(testConfiguration.getIndexName(), DEFAULT_FIELD_NAME, data, testConfiguration.indexDocumentCount);
        refreshIndex(testConfiguration.getIndexName());
        forceMergeKnnIndex(testConfiguration.getIndexName());
        log.info("Doc Count: {}", getDocCount(testConfiguration.getIndexName()));
    }

    @SneakyThrows
    private void validateBasicSearch(TestConfiguration testConfiguration) {
        if (testConfiguration.shouldRunBasic == false) {
            return;
        }
        for (int q = 0; q < testConfiguration.getQueryCount(); q++) {
            float[] queryVector = new float[testConfiguration.getDimension()];
            for (int j = 0; j < testConfiguration.getDimension(); j++) {
                queryVector[j] = randomFloat();
            }
            String query = buildQuery(testConfiguration, queryVector, null, false);
            validateSearch(testConfiguration.getIndexName(), query, testConfiguration.shouldBasicSearchWork);
        }
    }

    @SneakyThrows
    private void validateRescoreSearch(TestConfiguration testConfiguration) {
        if (testConfiguration.shouldRunRescore == false) {
            return;
        }
        for (int q = 0; q < testConfiguration.getQueryCount(); q++) {
            float[] queryVector = new float[testConfiguration.getDimension()];
            for (int j = 0; j < testConfiguration.getDimension(); j++) {
                queryVector[j] = randomFloat();
            }

            String query = buildQuery(testConfiguration, queryVector, null, true);
            validateSearch(testConfiguration.getIndexName(), query, testConfiguration.shouldRescoreSearchWork);
        }
    }

    @SneakyThrows
    private void validateSearch(String indexName, String query, boolean shouldWork) {
        if (shouldWork) {
            Response response = performSearch(indexName, query, "_source_excludes=" + DEFAULT_FIELD_NAME);
            log.info("Search Response: {}", responseAsMap(response));
            assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        } else {
            expectThrows(ResponseException.class, () -> performSearch(indexName, query));
        }
    }

    @SneakyThrows
    private void validateIndexDeletion(TestConfiguration testConfiguration) {
        if (testConfiguration.shouldDelete == false) {
            return;
        }
        deleteKNNIndex(testConfiguration.getIndexName());
    }

    @SneakyThrows
    private void validateModelDeletion(TestConfiguration testConfiguration) {
        if (testConfiguration.shouldDeleteModel == false || testConfiguration.modelId == null) {
            return;
        }
        deleteModel(testConfiguration.modelId);
    }

    @SneakyThrows
    private Settings createSettings(TestConfiguration testConfiguration) {
        if (testConfiguration.getSettings() != null) {
            return testConfiguration.getSettings();
        }

        return Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .put("index.knn", testConfiguration.isKNNSettingEnabled())
            .build();
    }

    @SneakyThrows
    private String createVectorMappings(TestConfiguration testConfiguration) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD)
            .startObject(DEFAULT_FIELD_NAME)
            .field(TYPE, TYPE_KNN_VECTOR);

        setIfNotNull(testConfiguration.getVectorDataType(), VECTOR_DATA_TYPE_FIELD, builder);
        if (testConfiguration.requiresTraining) {
            String modelId = randomAlphaOfLength(10).toLowerCase();
            log.info("ModelID: {}", modelId);
            builder.field(MODEL, modelId);
            return builder.endObject().endObject().endObject().toString();
        }

        builder.field(DIMENSION, testConfiguration.getDimension());
        if (testConfiguration.getMethodMappingBuilderConsumer() != null) {
            builder.startObject(KNN_METHOD);
            testConfiguration.getMethodMappingBuilderConsumer().accept(builder);
            builder.endObject();
        }
        setIfNotNull(testConfiguration.getMode(), MODE_PARAMETER, builder);
        setIfNotNull(testConfiguration.getCompression(), COMPRESSION_PARAMETER, builder);
        return builder.endObject().endObject().endObject().toString();
    }

    @SneakyThrows
    private String buildQuery(TestConfiguration testConfiguration, float[] floatVector, byte[] byteVector, boolean shouldAddRescore) {
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(QUERY)
            .startObject(KNN)
            .startObject(DEFAULT_FIELD_NAME)
            .field(VECTOR, floatVector)
            .field(K, 10);
        if (shouldAddRescore) {
            setIfNotNull(testConfiguration.getRescoreParam(), RESCORE_PARAMETER, builder);
            if (testConfiguration.getOversampleFactor() != null) {
                builder.startObject(RESCORE_PARAMETER)
                    .field(RESCORE_OVERSAMPLE_PARAMETER, testConfiguration.getOversampleFactor())
                    .endObject();
            }
        }
        setIfNotNull(testConfiguration.getSearchMethodParameters(), METHOD_PARAMETER, builder);

        return builder.endObject().endObject().endObject().endObject().toString();
    }

    @SneakyThrows
    private void setIfNotNull(Object value, String key, XContentBuilder builder) {
        if (value != null) {
            builder.field(key, value);
        }
    }

    @Getter
    @Builder
    private static class TestConfiguration {
        String testDescription;
        @Setter
        @Builder.Default
        String indexName = null;
        @Builder.Default
        String mode = null;
        @Builder.Default
        String compression = null;
        @Builder.Default
        Settings settings = null;
        @Builder.Default
        ThrowingConsumer<XContentBuilder> methodMappingBuilderConsumer = null;
        @Builder.Default
        boolean isKNNSettingEnabled = true;
        @Builder.Default
        boolean shouldRunRescore = true;
        @Builder.Default
        boolean shouldRunBasic = true;
        @Builder.Default
        boolean shouldDelete = true;
        @Builder.Default
        boolean shouldBasicSearchWork = true;
        @Builder.Default
        boolean shouldRescoreSearchWork = true;
        @Builder.Default
        String searchMethodParameters = null;
        @Builder.Default
        int dimension = DiskBasedFeatureIT.DEFAULT_DIMENSION;
        @Builder.Default
        String vectorDataType = null;
        @Builder.Default
        boolean requiresTraining = false;
        @Builder.Default
        int trainingDataRequired = 50;
        @Builder.Default
        int indexDocumentCount = 50;
        @Builder.Default
        int queryCount = 10;
        @Builder.Default
        boolean isNested = false;
        @Builder.Default
        boolean duplicateField = false;
        @Builder.Default
        boolean addRandomOtherField = false;
        @Builder.Default
        boolean addFilter = false;
        @Builder.Default
        boolean isRadialApplicable = false;
        @Builder.Default
        Integer oversampleFactor = null;
        @Builder.Default
        Boolean rescoreParam = null;
        @Builder.Default
        boolean shouldDeleteModel = true;
        @Builder.Default
        String modelId = null;
    }
}
