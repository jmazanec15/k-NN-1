/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.lucene;

import com.google.common.collect.ImmutableSet;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.List;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.DYNAMIC_CONFIDENCE_INTERVAL;
import static org.opensearch.knn.common.KNNConstants.ENCODER_SQ;
import static org.opensearch.knn.common.KNNConstants.LUCENE_SQ_BITS;
import static org.opensearch.knn.common.KNNConstants.LUCENE_SQ_CONFIDENCE_INTERVAL;
import static org.opensearch.knn.common.KNNConstants.LUCENE_SQ_DEFAULT_BITS;
import static org.opensearch.knn.common.KNNConstants.MAXIMUM_CONFIDENCE_INTERVAL;
import static org.opensearch.knn.common.KNNConstants.MINIMUM_CONFIDENCE_INTERVAL;

/**
 * Lucene scalar quantization encoder
 */
public class LuceneSQEncoder implements Encoder {
    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT);

    private final static List<Integer> LUCENE_SQ_BITS_SUPPORTED = List.of(7);
    private final static MethodComponent METHOD_COMPONENT = MethodComponent.Builder.builder(ENCODER_SQ)
        .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
        .addParameter(LUCENE_SQ_CONFIDENCE_INTERVAL, new Parameter.DoubleParameter(LUCENE_SQ_CONFIDENCE_INTERVAL, (v, context) -> {
            Double vResolved = v;
            if (vResolved == null) {
                vResolved = (double) DYNAMIC_CONFIDENCE_INTERVAL;
            }
            context.getLibraryParameters().put(LUCENE_SQ_CONFIDENCE_INTERVAL, vResolved);
            return null;
        }, v -> {
            if (v == null) {
                return null;
            }
            if (v == DYNAMIC_CONFIDENCE_INTERVAL || (v >= MINIMUM_CONFIDENCE_INTERVAL && v <= MAXIMUM_CONFIDENCE_INTERVAL)) {
                return null;
            }
            return ValidationUtil.chainValidationErrors(null, "Invalid confidence interval. IMPROVE");
        }))
        .addParameter(LUCENE_SQ_BITS, new Parameter.IntegerParameter(LUCENE_SQ_BITS, (v, context) -> {
            Integer vResolved = v;
            if (vResolved == null) {
                vResolved = LUCENE_SQ_DEFAULT_BITS;
            }
            context.getLibraryParameters().put(LUCENE_SQ_BITS, vResolved);
            return null;
        }, v -> {
            if (v == null) {
                return null;
            }
            if (LUCENE_SQ_BITS_SUPPORTED.contains(v)) {
                return null;
            }
            return ValidationUtil.chainValidationErrors(null, "Invalid confidence interval. IMPROVE");
        }))
        .build();

    @Override
    public MethodComponent getMethodComponent() {
        return METHOD_COMPONENT;
    }
}
