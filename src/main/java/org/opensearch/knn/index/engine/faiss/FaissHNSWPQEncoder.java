/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import com.google.common.collect.ImmutableSet;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

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
        .addParameter(ENCODER_PARAMETER_PQ_M, new Parameter.IntegerParameter(ENCODER_PARAMETER_PQ_M, (v, context) -> {
            Integer vResolved = v;
            if (vResolved == null) {
                vResolved = ENCODER_PARAMETER_PQ_CODE_COUNT_DEFAULT;
            }

            ValidationException validationException = ValidationUtil.chainValidationErrors(
                null,
                context.getDimension() % vResolved == 0 ? "vvdf" : null
            );
            if (validationException != null) {
                return validationException;
            }

            context.getLibraryParameters().put(ENCODER_PARAMETER_PQ_M, vResolved);
            return null;
        }, v -> {
            boolean isValueGreaterThan0 = v > 0;
            boolean isValueLessThanCodeCountLimit = v < ENCODER_PARAMETER_PQ_CODE_COUNT_LIMIT;
            return ValidationUtil.chainValidationErrors(null, isValueGreaterThan0 && isValueLessThanCodeCountLimit ? "vvdf" : null);
        }))
        .addParameter(ENCODER_PARAMETER_PQ_CODE_SIZE, new Parameter.IntegerParameter(ENCODER_PARAMETER_PQ_CODE_SIZE, (v, context) -> {
            Integer vResolved = v;
            if (vResolved == null) {
                vResolved = ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT;
            }
            context.getLibraryParameters().put(ENCODER_PARAMETER_PQ_CODE_SIZE, vResolved);
            return null;
        }, v -> {
            if (v == null) {
                return null;
            }
            boolean isValueNotDefault = !Objects.equals(v, ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT);
            return ValidationUtil.chainValidationErrors(null, isValueNotDefault ? "Value must be ADD_ME" : null);
        }))
        .setRequiresTraining(true)
        .setPostResolveProcessor(
            ((methodComponent, contextParamMap, knnIndexContext) -> IndexDescriptionPostResolveProcessor.builder(
                "," + FAISS_PQ_DESCRIPTION,
                methodComponent,
                knnIndexContext,
                contextParamMap
            ).addParameter(ENCODER_PARAMETER_PQ_M, "", "").addParameter(ENCODER_PARAMETER_PQ_CODE_SIZE, "x", "").build())
        )
        .setOverheadInKBEstimator((methodComponent, methodComponentContext, knnIndexContext) -> {
            int codeSize = ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT;
            return Math.toIntExact(((4L * (1L << codeSize) * knnIndexContext.getDimension()) / BYTES_PER_KILOBYTES) + 1);
        })
        .build();

    @Override
    public MethodComponent getMethodComponent() {
        return METHOD_COMPONENT;
    }
}
