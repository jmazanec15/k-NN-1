/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import com.google.common.collect.ImmutableSet;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.FilterKNNLibrarySearchContext;
import org.opensearch.knn.index.engine.KNNIndexContext;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.engine.validation.ValidationUtil;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.knn.quantization.enums.ScalarQuantizationType;

import java.util.Locale;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.PARAMETERS;

/**
 * Quantization framework binary encoder,
 */
public class QFrameBitEncoder implements Encoder {

    public static final String NAME = "binary";
    public static final String BITCOUNT_PARAM = "bits";
    private static final int DEFAULT_BITS = 1;
    private static final Set<Integer> validBitCounts = ImmutableSet.of(1, 2, 4);
    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT);

    /**
     * {
     *   "encoder": {
     *     "name": "binary",
     *     "parameters": {
     *       "bits": 2
     *     }
     *   }
     * }
     */
    private final static MethodComponent METHOD_COMPONENT = MethodComponent.Builder.builder(NAME)
        .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
        .addParameter(BITCOUNT_PARAM, new Parameter.IntegerParameter(BITCOUNT_PARAM, (v, context) -> {
            int vResolved = resolveBitCount(context, v);
            context.setQuantizationConfig(resolveQuantizationConfig(vResolved));
            context.getLibraryParameters().put(KNNConstants.VECTOR_DATA_TYPE_FIELD, VectorDataType.BINARY.getValue());
            RescoreContext rescoreContext = resolveRescoreContextFromBitCount(vResolved);
            if (rescoreContext != null) {
                context.setKnnLibrarySearchContext(new FilterKNNLibrarySearchContext(context.getKnnLibrarySearchContext()) {
                    @Override
                    public RescoreContext getDefaultRescoreContext(QueryContext ctx) {
                        return rescoreContext;
                    }
                });
            }
            return null;
        },
            (v) -> ValidationUtil.chainValidationErrors(
                null,
                v == null || validBitCounts.contains(v) ? null : String.format(Locale.ROOT, "Invalid bit count: %d", v)
            )
        ))
        .setPostResolveProcessor(((methodComponent, knnIndexContext) -> {
            // We dont need the parameters any more. Lets remove
            //TODO: We should clarify when we remove
            knnIndexContext.getLibraryParameters().remove(PARAMETERS);
            return null;
        }))
        .setRequiresTraining(false)
        .build();

    @Override
    public MethodComponent getMethodComponent() {
        return METHOD_COMPONENT;
    }

    private static int resolveBitCount(KNNIndexContext knnIndexContext, Integer bitCount) {
        if (bitCount != null) {
            return bitCount;
        }

        CompressionConfig compressionConfig = knnIndexContext.getResolvedRequiredParameters().getCompressionConfig();
        if (compressionConfig.equals(CompressionConfig.NOT_CONFIGURED)) {
            return DEFAULT_BITS;
        }

        int level = compressionConfig.getCompressionLevel();
        if (level == 32) {
            return 1;
        }

        if (level == 16) {
            return 2;
        }

        if (level == 8) {
            return 4;
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid bit count: %d", bitCount));
    }

    private static QuantizationConfig resolveQuantizationConfig(int bitCount) {
        if (bitCount == 1) {
            return QuantizationConfig.builder().quantizationType(ScalarQuantizationType.ONE_BIT).build();
        }

        if (bitCount == 2) {
            return QuantizationConfig.builder().quantizationType(ScalarQuantizationType.TWO_BIT).build();
        }

        if (bitCount == 4) {
            return QuantizationConfig.builder().quantizationType(ScalarQuantizationType.FOUR_BIT).build();
        }

        throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid bit count: %d", bitCount));
    }

    private static RescoreContext resolveRescoreContextFromBitCount(int bitCount) {
        if (bitCount == 1) {
            return RescoreContext.builder().oversampleFactor(5).build();
        }

        if (bitCount == 2) {
            return RescoreContext.builder().oversampleFactor(3).build();
        }

        if (bitCount == 4) {
            return RescoreContext.builder().oversampleFactor(1.5f).build();
        }

        return null;
    }
}
