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
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@Log4j2
@AllArgsConstructor
public class SyntheticVectorInjectionConsumer implements BiFunction<Integer, byte[], byte[]> {

    private final Map<String, Supplier<KNNVectorValues<?>>> vectorValuesSuppliers;

    @Override
    public byte[] apply(Integer integer, byte[] bytes) {
        try {
            Tuple<? extends MediaType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(
                BytesReference.fromByteBuffer(ByteBuffer.wrap(bytes)),
                true,
                MediaTypeRegistry.getDefaultMediaType()
            );
            Map<String, Object> sourceAsMap = new HashMap<>(mapTuple.v2());
            for (Map.Entry<String, Supplier<KNNVectorValues<?>>> entry : vectorValuesSuppliers.entrySet()) {
                log.info("Injecting vector values for field: " + entry.getKey());
                KNNVectorValues<?> vectorValues = entry.getValue().get();
                vectorValues.advance(integer);
                sourceAsMap.put(entry.getKey(), vectorValues.getVector());
            }
            BytesStreamOutput bStream = new BytesStreamOutput(1024);
            MediaType actualContentType = mapTuple.v1();
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(actualContentType, bStream).map(sourceAsMap);
            builder.close();
            log.info("Built the following source: " + builder);
            return BytesReference.toBytes(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
