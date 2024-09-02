/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import com.google.common.collect.ImmutableSet;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.ENCODER_SQ;
import static org.opensearch.knn.common.KNNConstants.FAISS_SQ_CLIP;
import static org.opensearch.knn.common.KNNConstants.FAISS_SQ_DESCRIPTION;
import static org.opensearch.knn.common.KNNConstants.FAISS_SQ_ENCODER_FP16;
import static org.opensearch.knn.common.KNNConstants.FAISS_SQ_ENCODER_TYPES;
import static org.opensearch.knn.common.KNNConstants.FAISS_SQ_TYPE;
import static org.opensearch.knn.index.engine.faiss.FaissFP16Util.CLIP_TO_FP16_PROCESSOR;
import static org.opensearch.knn.index.engine.faiss.FaissFP16Util.FP16_VALIDATOR;

/**
 * Faiss SQ encoder
 */
public class FaissSQEncoder implements Encoder {

    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT);

    private final static MethodComponent METHOD_COMPONENT = MethodComponent.Builder.builder(ENCODER_SQ)
        .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
        .addParameter(FAISS_SQ_TYPE, new Parameter.StringParameter(FAISS_SQ_TYPE, (v, builder) -> {
            String vResolved = v;
            if (vResolved == null) {
                vResolved = FAISS_SQ_ENCODER_FP16;
            }
            if (FAISS_SQ_ENCODER_FP16.equals(vResolved) == false && builder.getPerDimensionProcessor() == CLIP_TO_FP16_PROCESSOR) {
                builder.addValidationErrorMessage("Clip only supported for FP16 encoder.", true);
            }

            if (FAISS_SQ_ENCODER_FP16.equals(vResolved)) {
                builder.perDimensionValidator(FP16_VALIDATOR);
            }

            builder.getLibraryParameters().put(FAISS_SQ_TYPE, vResolved);
        }, v -> {
            if (v == null) {
                return null;
            }
            if (FAISS_SQ_ENCODER_TYPES.contains(v)) {
                return null;
            }
            return ValidationUtil.chainValidationErrors(null, "Invalid encoder type. IMPROVE");
        }))
        .addParameter(FAISS_SQ_CLIP, new Parameter.BooleanParameter(FAISS_SQ_CLIP, (v, builder) -> {
            Boolean vResolved = v;
            if (vResolved == null) {
                vResolved = false;
            }
            if (vResolved
                && builder.getLibraryParameters() != null
                && builder.getLibraryParameters().get(FAISS_SQ_TYPE) != FAISS_SQ_ENCODER_FP16) {
                builder.addValidationErrorMessage("Clip only supported for FP16 encoder.", true);
            }
            if (vResolved) {
                builder.perDimensionProcessor(CLIP_TO_FP16_PROCESSOR);
            }
        }, v -> null))
        .setPostResolveProcessor(
            ((methodComponent, knnIndexContext) -> IndexDescriptionPostResolveProcessor.builder(
                "," + FAISS_SQ_DESCRIPTION,
                methodComponent,
                knnIndexContext
            ).addParameter(FAISS_SQ_TYPE, "", "").build())
        )
        .build();

    @Override
    public MethodComponent getMethodComponent() {
        return METHOD_COMPONENT;
    }
}
