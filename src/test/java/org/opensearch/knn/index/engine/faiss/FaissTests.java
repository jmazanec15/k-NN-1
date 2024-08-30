/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import org.opensearch.knn.KNNTestCase;

public class FaissTests extends KNNTestCase {
    //
    // public void testGetKNNLibraryIndexingContext_whenMethodIsHNSWFlat_thenCreateCorrectIndexDescription() throws IOException {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    //
    // int mParam = 65;
    // String expectedIndexDescription = String.format(Locale.ROOT, "HNSW%d,Flat", mParam);
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_HNSW)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_M, mParam)
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    //
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // Map<String, Object> map = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext).getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // }
    //
    // public void testGetKNNLibraryIndexingContext_whenMethodIsHNSWPQ_thenCreateCorrectIndexDescription() throws IOException {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int hnswMParam = 65;
    // int pqMParam = 17;
    // String expectedIndexDescription = String.format(Locale.ROOT, "HNSW%d,PQ%d", hnswMParam, pqMParam);
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_HNSW)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_M, hnswMParam)
    // .startObject(METHOD_ENCODER_PARAMETER)
    // .field(NAME, ENCODER_PQ)
    // .startObject(PARAMETERS)
    // .field(ENCODER_PARAMETER_PQ_M, pqMParam)
    // .endObject()
    // .endObject()
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // Map<String, Object> map = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext).getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // }
    //
    // @SneakyThrows
    // public void testGetKNNLibraryIndexingContext_whenMethodIsHNSWSQFP16_thenCreateCorrectIndexDescription() {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int hnswMParam = 65;
    // String expectedIndexDescription = String.format(Locale.ROOT, "HNSW%d,SQfp16", hnswMParam);
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_HNSW)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_M, hnswMParam)
    // .startObject(METHOD_ENCODER_PARAMETER)
    // .field(NAME, ENCODER_SQ)
    // .startObject(PARAMETERS)
    // .field(FAISS_SQ_TYPE, FAISS_SQ_ENCODER_FP16)
    // .endObject()
    // .endObject()
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // Map<String, Object> map = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext).getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // }
    //
    // public void testGetKNNLibraryIndexingContext_whenMethodIsIVFFlat_thenCreateCorrectIndexDescription() throws IOException {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int nlists = 88;
    // String expectedIndexDescription = String.format(Locale.ROOT, "IVF%d,Flat", nlists);
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_IVF)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_NLIST, nlists)
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // Map<String, Object> map = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext).getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // }
    //
    // public void testGetKNNLibraryIndexingContext_whenMethodIsIVFPQ_thenCreateCorrectIndexDescription() throws IOException {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int ivfNlistsParam = 88;
    // int pqMParam = 17;
    // int pqCodeSizeParam = 53;
    // String expectedIndexDescription = String.format(Locale.ROOT, "IVF%d,PQ%dx%d", ivfNlistsParam, pqMParam, pqCodeSizeParam);
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_IVF)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_NLIST, ivfNlistsParam)
    // .startObject(METHOD_ENCODER_PARAMETER)
    // .field(NAME, ENCODER_PQ)
    // .startObject(PARAMETERS)
    // .field(ENCODER_PARAMETER_PQ_M, pqMParam)
    // .field(ENCODER_PARAMETER_PQ_CODE_SIZE, pqCodeSizeParam)
    // .endObject()
    // .endObject()
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // Map<String, Object> map = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext).getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // }
    //
    // @SneakyThrows
    // public void testGetKNNLibraryIndexingContext_whenMethodIsIVFSQFP16_thenCreateCorrectIndexDescription() {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int nlists = 88;
    // String expectedIndexDescription = String.format(Locale.ROOT, "IVF%d,SQfp16", nlists);
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_IVF)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_NLIST, nlists)
    // .startObject(METHOD_ENCODER_PARAMETER)
    // .field(NAME, ENCODER_SQ)
    // .startObject(PARAMETERS)
    // .field(FAISS_SQ_TYPE, FAISS_SQ_ENCODER_FP16)
    // .endObject()
    // .endObject()
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // Map<String, Object> map = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext).getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // }
    //
    // @SneakyThrows
    // public void testGetKNNLibraryIndexingContext_whenMethodIsHNSWWithQFrame_thenCreateCorrectConfig() {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int m = 88;
    // String expectedIndexDescription = "BHNSW" + m + ",Flat";
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_HNSW)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_M, m)
    // .startObject(METHOD_ENCODER_PARAMETER)
    // .field(NAME, QFrameBitEncoder.NAME)
    // .startObject(PARAMETERS)
    // .field(QFrameBitEncoder.BITCOUNT_PARAM, 4)
    // .endObject()
    // .endObject()
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // KNNLibraryIndexingContext knnLibraryIndexingContext = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext);
    // Map<String, Object> map = knnLibraryIndexingContext.getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(
    // QuantizationConfig.builder().quantizationType(ScalarQuantizationType.FOUR_BIT).build(),
    // knnLibraryIndexingContext.getQuantizationConfig()
    // );
    // }
    //
    // @SneakyThrows
    // public void testGetKNNLibraryIndexingContext_whenMethodIsIVFWithQFrame_thenCreateCorrectConfig() {
    // KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
    // .versionCreated(org.opensearch.Version.CURRENT)
    // .dimension(4)
    // .vectorDataType(VectorDataType.FLOAT)
    // .build();
    // int nlist = 88;
    // String expectedIndexDescription = "BIVF" + nlist + ",Flat";
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, METHOD_IVF)
    // .field(KNN_ENGINE, FAISS_NAME)
    // .startObject(PARAMETERS)
    // .field(METHOD_PARAMETER_NLIST, nlist)
    // .startObject(METHOD_ENCODER_PARAMETER)
    // .field(NAME, QFrameBitEncoder.NAME)
    // .startObject(PARAMETERS)
    // .field(QFrameBitEncoder.BITCOUNT_PARAM, 2)
    // .endObject()
    // .endObject()
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);
    // knnMethodConfigContext.setKnnMethodContext(knnMethodContext);
    // KNNLibraryIndexingContext knnLibraryIndexingContext = Faiss.INSTANCE.getKNNLibraryIndexingContext(knnMethodConfigContext);
    // Map<String, Object> map = knnLibraryIndexingContext.getLibraryParameters();
    //
    // assertTrue(map.containsKey(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(expectedIndexDescription, map.get(INDEX_DESCRIPTION_PARAMETER));
    // assertEquals(
    // QuantizationConfig.builder().quantizationType(ScalarQuantizationType.TWO_BIT).build(),
    // knnLibraryIndexingContext.getQuantizationConfig()
    // );
    // }
    //
    // public void testMethodAsMapBuilder() throws IOException {
    // String methodName = "test-method";
    // String methodDescription = "test-description";
    // String parameter1 = "test-parameter-1";
    // Integer value1 = 10;
    // Integer defaultValue1 = 1;
    // String parameter2 = "test-parameter-2";
    // Integer value2 = 15;
    // Integer defaultValue2 = 2;
    // String parameter3 = "test-parameter-3";
    // Integer defaultValue3 = 3;
    // MethodComponent methodComponent = MethodComponent.Builder.builder(methodName)
    // .addParameter(parameter1, new Parameter.IntegerParameter(parameter1, k -> defaultValue1, (value, context) -> value > 0))
    // .addParameter(parameter2, new Parameter.IntegerParameter(parameter2, k -> defaultValue2, (value, context) -> value > 0))
    // .addParameter(parameter3, new Parameter.IntegerParameter(parameter3, k -> defaultValue3, (value, context) -> value > 0))
    // .build();
    //
    // XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
    // .startObject()
    // .field(NAME, methodName)
    // .startObject(PARAMETERS)
    // .field(parameter1, value1)
    // .field(parameter2, value2)
    // .endObject()
    // .endObject();
    // Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
    // MethodComponentContext methodComponentContext = MethodComponentContext.parse(in);
    //
    // Map<String, Object> expectedParametersMap = new HashMap<>(methodComponentContext.getParameters().orElse(Collections.emptyMap()));
    // expectedParametersMap.put(parameter3, defaultValue3);
    // expectedParametersMap.remove(parameter1);
    // Map<String, Object> expectedMap = new HashMap<>();
    // expectedMap.put(PARAMETERS, expectedParametersMap);
    // expectedMap.put(NAME, methodName);
    // expectedMap.put(INDEX_DESCRIPTION_PARAMETER, methodDescription + value1);
    // KNNLibraryIndexingContext expectedKNNMethodContext = KNNLibraryIndexingContextImpl.builder().parameters(expectedMap).build();
    //
    // KNNLibraryIndexingContext actualKNNLibraryIndexingContext = IndexDescriptionPostResolveProcessor.builder(
    // methodDescription,
    // methodComponent,
    // methodComponentContext,
    // KNNMethodConfigContext.builder().versionCreated(Version.CURRENT).build()
    // ).addParameter(parameter1, "", "").build();
    //
    // assertEquals(expectedKNNMethodContext.getQuantizationConfig(), actualKNNLibraryIndexingContext.getQuantizationConfig());
    // assertEquals(expectedKNNMethodContext.getLibraryParameters(), actualKNNLibraryIndexingContext.getLibraryParameters());
    // assertEquals(expectedKNNMethodContext.getPerDimensionProcessor(), actualKNNLibraryIndexingContext.getPerDimensionProcessor());
    // assertEquals(expectedKNNMethodContext.getPerDimensionValidator(), actualKNNLibraryIndexingContext.getPerDimensionValidator());
    // assertEquals(expectedKNNMethodContext.getVectorValidator(), actualKNNLibraryIndexingContext.getVectorValidator());
    // }

}
