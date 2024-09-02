/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import com.google.common.collect.ImmutableSet;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.BYTES_PER_KILOBYTES;
import static org.opensearch.knn.common.KNNConstants.ENCODER_PARAMETER_PQ_CODE_COUNT_DEFAULT;
import static org.opensearch.knn.common.KNNConstants.ENCODER_PARAMETER_PQ_CODE_COUNT_LIMIT;
import static org.opensearch.knn.common.KNNConstants.ENCODER_PARAMETER_PQ_CODE_SIZE;
import static org.opensearch.knn.common.KNNConstants.ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT;
import static org.opensearch.knn.common.KNNConstants.ENCODER_PARAMETER_PQ_M;
import static org.opensearch.knn.common.KNNConstants.FAISS_PQ_DESCRIPTION;

/**
 * Faiss HNSW PQ encoder. Right now, the implementations are slightly different during validation between this an
 * {@link FaissIVFPQEncoder}. Hence, they are separate classes.
 */
public class FaissHNSWPQEncoder implements Encoder {

    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT);

    private final static MethodComponent METHOD_COMPONENT = MethodComponent.Builder.builder(KNNConstants.ENCODER_PQ)
        .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
        .addParameter(ENCODER_PARAMETER_PQ_M, new Parameter.IntegerParameter(ENCODER_PARAMETER_PQ_M, (v, builder) -> {
            Integer vResolved = v;
            if (vResolved == null) {
                vResolved = ENCODER_PARAMETER_PQ_CODE_COUNT_DEFAULT;
            }

            if (builder.getKnnLibraryIndexConfig().getDimension() % vResolved == 0) {
                builder.addValidationErrorMessage(
                    String.format(
                        Locale.ROOT,
                        "Invalid parameter for m parameter of product quantization: dimension \"[%d]\" must be divisible by m \"[%d]\"",
                        builder.getKnnLibraryIndexConfig().getDimension(),
                        vResolved
                    )
                );
            }
            builder.getLibraryParameters().put(ENCODER_PARAMETER_PQ_M, vResolved);
        }, v -> {
            if (v == null) {
                return null;
            }
            boolean isValueGreaterThan0 = v > 0;
            boolean isValueLessThanCodeCountLimit = v < ENCODER_PARAMETER_PQ_CODE_COUNT_LIMIT;
            return ValidationUtil.chainValidationErrors(
                null,
                isValueGreaterThan0 && isValueLessThanCodeCountLimit
                    ? null
                    : String.format(
                        Locale.ROOT,
                        "Invalid parameter for m parameter of product quantization: m \"[%d]\"  must be greater than 0 and less than \"[%d]\"",
                        v,
                        ENCODER_PARAMETER_PQ_CODE_COUNT_LIMIT
                    )
            );
        }))
        .addParameter(ENCODER_PARAMETER_PQ_CODE_SIZE, new Parameter.IntegerParameter(ENCODER_PARAMETER_PQ_CODE_SIZE, (v, context) -> {
            Integer vResolved = v;
            if (vResolved == null) {
                vResolved = ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT;
            }
            context.getLibraryParameters().put(ENCODER_PARAMETER_PQ_CODE_SIZE, vResolved);
        }, v -> {
            if (v == null) {
                return null;
            }
            boolean isValueDefault = Objects.equals(v, ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT);
            return ValidationUtil.chainValidationErrors(
                null,
                isValueDefault
                    ? null
                    : String.format(
                        Locale.ROOT,
                        "Invalid parameter for code_size parameter of product quantization: code_size \"[%d]\"  must be \"[%d]\"",
                        v,
                        ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT
                    )
            );
        }))
        .setRequiresTraining(true)
        .setPostResolveProcessor(((methodComponent, builder) -> {
            int codeSize = ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT;
            builder.incEstimatedIndexOverhead(
                Math.toIntExact(((4L * (1L << codeSize) * builder.getKnnLibraryIndexConfig().getDimension()) / BYTES_PER_KILOBYTES) + 1)
            );
            IndexDescriptionPostResolveProcessor.builder("," + FAISS_PQ_DESCRIPTION, methodComponent, builder)
                .addParameter(ENCODER_PARAMETER_PQ_M, "", "")
                .addParameter(ENCODER_PARAMETER_PQ_CODE_SIZE, "x", "")
                .build();
        }))
        .build();

    @Override
    public MethodComponent getMethodComponent() {
        return METHOD_COMPONENT;
    }
}
