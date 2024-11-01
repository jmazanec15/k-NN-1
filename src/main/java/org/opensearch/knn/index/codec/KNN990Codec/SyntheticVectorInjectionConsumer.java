/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Given a document id and the source of the doc, add the vectors from the different fields into the mapping.
 */
@Log4j2
@AllArgsConstructor
public class SyntheticVectorInjectionConsumer implements BiFunction<Integer, byte[], byte[]> {

    List<PerFieldSyntheticVectorInjector> vectorInjectors;

    /**
     * Adds vectors into source
     *
     * @param docId the first function argument
     * @param sourceAsBytes the second function argument
     * @return new byte array
     */
    @Override
    public byte[] apply(Integer docId, byte[] sourceAsBytes) {
        try {
            Tuple<? extends MediaType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(
                BytesReference.fromByteBuffer(ByteBuffer.wrap(sourceAsBytes)),
                true,
                MediaTypeRegistry.getDefaultMediaType()
            );

            Map<String, Object> sourceAsMap = new HashMap<>(mapTuple.v2());
            for (PerFieldSyntheticVectorInjector vectorInjector : vectorInjectors) {
                //log.info("Injecting vector values for field: " + vectorInjector.getFieldName());
                vectorInjector.accept(docId, sourceAsMap);
            }

            BytesStreamOutput bStream = new BytesStreamOutput(1024);
            MediaType actualContentType = mapTuple.v1();
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(actualContentType, bStream).map(sourceAsMap);
            builder.close();
            // log.info("Built the following source: " + builder);
            return BytesReference.toBytes(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
