/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.SneakyThrows;
import org.apache.lucene.index.FloatVectorValues;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;
import org.opensearch.knn.index.vectorvalues.TestVectorValues;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class SyntheticVectorInjectionConsumerTests extends KNNTestCase {

    @SneakyThrows
    public void testVectorInjection() {
        FloatVectorValues randomVectorValues = new TestVectorValues.PreDefinedFloatVectorValues(
            List.of(new float[] { 1.0f, 2.0f }, new float[] { 2.0f, 3.0f }, new float[] { 3.0f, 4.0f }, new float[] { 4.0f, 5.0f })
        );
        final KNNVectorValues<float[]> knnVectorValues = KNNVectorValuesFactory.getVectorValues(VectorDataType.FLOAT, randomVectorValues);

        final XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("test_text", "text-field");
        builder.endObject();

        BytesReference bytesReference = BytesReference.bytes(builder);
        toMap(bytesReference);

        SyntheticVectorInjectionConsumer consumer = new SyntheticVectorInjectionConsumer(Map.of("test_vector", () -> knnVectorValues));
        logger.info(bytesReference.length());
        byte[] modifiedBytes = consumer.apply(0, bytesReference.toBytesRef().bytes);
        BytesReference modifiedBytesReference = BytesReference.fromByteBuffer(ByteBuffer.wrap(modifiedBytes));
        toMap(modifiedBytesReference);

        modifiedBytes = consumer.apply(1, bytesReference.toBytesRef().bytes);
        modifiedBytesReference = BytesReference.fromByteBuffer(ByteBuffer.wrap(modifiedBytes));
        toMap(modifiedBytesReference);

        modifiedBytes = consumer.apply(0, bytesReference.toBytesRef().bytes);
        modifiedBytesReference = BytesReference.fromByteBuffer(ByteBuffer.wrap(modifiedBytes));
        toMap(modifiedBytesReference);

        fail("On purpose");
    }

    private void toMap(BytesReference source) {
        Tuple<? extends MediaType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, true, MediaTypeRegistry.JSON);
        logger.info(mapTuple.v2().toString());
    }

}
