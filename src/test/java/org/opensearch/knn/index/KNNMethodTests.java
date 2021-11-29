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

package org.opensearch.knn.index;

import com.google.common.collect.ImmutableMap;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNMethod;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.engine.MethodComponent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.NAME;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;


public class KNNMethodTests extends KNNTestCase {
    /**
     * Test KNNMethod method component getter
     */
    public void testGetMethodComponent() {
        String name = "test";
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .build();
        assertEquals(name, knnMethod.getMethodComponent().getName());
    }

    /**
     * Test KNNMethod has space
     */
    public void testHasSpace() {
        String name = "test";
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .addSpaces(SpaceType.L2, SpaceType.COSINESIMIL)
                .build();
        assertTrue(knnMethod.containsSpace(SpaceType.L2));
        assertTrue(knnMethod.containsSpace(SpaceType.COSINESIMIL));
        assertFalse(knnMethod.containsSpace(SpaceType.INNER_PRODUCT));
    }

    /**
     * Test KNNMethod validate
     */
    public void testValidate() throws IOException {
        String methodName = "test-method";
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(methodName).build())
                .addSpaces(SpaceType.L2)
                .build();

        // Invalid space
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(METHOD_PARAMETER_SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext1 = KNNMethodContext.parse(in);
        assertNotNull(knnMethod.validate(knnMethodContext1));

        // Invalid methodComponent
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(METHOD_PARAMETER_SPACE_TYPE, SpaceType.L2.getValue())
                .startObject(PARAMETERS)
                .field("invalid", "invalid")
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext2 = KNNMethodContext.parse(in);

        assertNotNull(knnMethod.validate(knnMethodContext2));

        // Valid everything
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(METHOD_PARAMETER_SPACE_TYPE, SpaceType.L2.getValue())
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext3 = KNNMethodContext.parse(in);
        assertNull(knnMethod.validate(knnMethodContext3));
    }

    public void testGetAsMap() {
        SpaceType spaceType = SpaceType.DEFAULT;
        String methodName = "test-method";
        Map<String, Object> generatedMap = ImmutableMap.of("test-key", "test-value");
        MethodComponent methodComponent = MethodComponent.Builder.builder(methodName)
                .setMapGenerator(((methodComponent1, methodComponentContext) -> generatedMap))
                .build();

        KNNMethod knnMethod = KNNMethod.Builder.builder(methodComponent).build();

        Map<String, Object> expectedMap = new HashMap<>(generatedMap);
        expectedMap.put(KNNConstants.SPACE_TYPE, spaceType.getValue());

        assertEquals(expectedMap, knnMethod.getAsMap(new KNNMethodContext(KNNEngine.DEFAULT, spaceType, null)));
    }

    public void testBuilder() {
        String name = "test";
        KNNMethod.Builder builder = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build());
        KNNMethod knnMethod = builder.build();

        assertEquals(name, knnMethod.getMethodComponent().getName());

        builder.addSpaces(SpaceType.L2);
        knnMethod = builder.build();

        assertTrue(knnMethod.containsSpace(SpaceType.L2));
    }
}
