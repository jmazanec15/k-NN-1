/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import com.google.common.collect.ImmutableSet;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.AbstractKNNMethod;
import org.opensearch.knn.index.engine.DefaultIVFSearchContext;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.knn.common.KNNConstants.BYTES_PER_KILOBYTES;
import static org.opensearch.knn.common.KNNConstants.FAISS_IVF_DESCRIPTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_ENCODER_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_IVF;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NLIST;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NLIST_DEFAULT;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NLIST_LIMIT;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES_DEFAULT;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES_LIMIT;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;

/**
 * Faiss ivf implementation
 */
public class FaissIVFMethod extends AbstractFaissMethod {

    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT, VectorDataType.BINARY);

    public final static List<SpaceType> SUPPORTED_SPACES = Arrays.asList(SpaceType.L2, SpaceType.INNER_PRODUCT, SpaceType.HAMMING);

    private final static MethodComponentContext DEFAULT_ENCODER_CONTEXT = new MethodComponentContext(
        KNNConstants.ENCODER_FLAT,
        Collections.emptyMap()
    );
    private final static MethodComponentContext DEFAULT_32x_ENCODER_CONTEXT = new MethodComponentContext(
        QFrameBitEncoder.NAME,
        Map.of(QFrameBitEncoder.BITCOUNT_PARAM, 1)
    );
    private final static MethodComponentContext DEFAULT_16x_ENCODER_CONTEXT = new MethodComponentContext(
        QFrameBitEncoder.NAME,
        Map.of(QFrameBitEncoder.BITCOUNT_PARAM, 2)
    );
    private final static MethodComponentContext DEFAULT_8x_ENCODER_CONTEXT = new MethodComponentContext(
        QFrameBitEncoder.NAME,
        Map.of(QFrameBitEncoder.BITCOUNT_PARAM, 4)
    );

    private final static List<Encoder> SUPPORTED_ENCODERS = List.of(
        new FaissFlatEncoder(),
        new FaissSQEncoder(),
        new FaissIVFPQEncoder(),
        new QFrameBitEncoder()
    );

    /**
     * Constructor for FaissIVFMethod
     *
     * @see AbstractKNNMethod
     */
    public FaissIVFMethod() {
        super(initMethodComponent(), Set.copyOf(SUPPORTED_SPACES), new DefaultIVFSearchContext());
    }

    private static MethodComponent initMethodComponent() {
        return MethodComponent.Builder.builder(METHOD_IVF)
            .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
            .addParameter(METHOD_PARAMETER_NPROBES, new Parameter.IntegerParameter(METHOD_PARAMETER_NPROBES, (v, context) -> {
                Integer vResolved = v;
                if (vResolved == null) {
                    vResolved = METHOD_PARAMETER_NPROBES_DEFAULT;
                }
                context.getLibraryParameters().put(METHOD_PARAMETER_NPROBES, vResolved);
                return null;
            }, v -> {
                if (v == null) {
                    return null;
                }
                boolean isValid = v > 0 && v < METHOD_PARAMETER_NPROBES_LIMIT;
                return ValidationUtil.chainValidationErrors(null, isValid ? null : "UPDATE ME");
            }))
            .addParameter(METHOD_PARAMETER_NLIST, new Parameter.IntegerParameter(METHOD_PARAMETER_NLIST, (v, context) -> {
                Integer vResolved = v;
                if (vResolved == null) {
                    vResolved = METHOD_PARAMETER_NLIST_DEFAULT;
                }
                context.getLibraryParameters().put(METHOD_PARAMETER_NLIST, vResolved);
                return null;
            }, v -> {
                if (v == null) {
                    return null;
                }
                boolean isValid = v > 0 && v < METHOD_PARAMETER_NLIST_LIMIT;
                return ValidationUtil.chainValidationErrors(null, isValid ? null : "UPDATE ME");
            }))
            .addParameter(METHOD_ENCODER_PARAMETER, initEncoderParameter())
            .setRequiresTraining(true)
            .setPostResolveProcessor(
                ((methodComponent, contextMap, knnIndexContext) -> IndexDescriptionPostResolveProcessor.builder(
                    FAISS_IVF_DESCRIPTION,
                    methodComponent,
                    knnIndexContext,
                    contextMap
                ).addParameter(METHOD_PARAMETER_NLIST, "", "").addParameter(METHOD_ENCODER_PARAMETER, "", "").build())
            )
            .setOverheadInKBEstimator((methodComponent, methodComponentContext, knnIndexContext) -> {
                int centroids = (Integer) ((Map<String, Object>) knnIndexContext.getLibraryParameters().get(PARAMETERS)).get(
                    METHOD_PARAMETER_NLIST
                );
                return Math.toIntExact(((4L * centroids * knnIndexContext.getDimension()) / BYTES_PER_KILOBYTES) + 1);
            })
            .build();
    }

    private static Parameter.MethodComponentContextParameter initEncoderParameter() {
        return new Parameter.MethodComponentContextParameter(METHOD_ENCODER_PARAMETER, (v, context) -> {
            MethodComponentContext vResolved = v;
            if (vResolved == null) {
                vResolved = getDefaultEncoderFromCompression(
                    context.getResolvedRequiredParameters().getCompressionConfig(),
                    context.getResolvedRequiredParameters().getMode()
                );
            }

            if (vResolved.getName().isEmpty()) {
                if (vResolved.getParameters().isPresent()) {
                    return ValidationUtil.chainValidationErrors(null, "Invalid configuration. Need to specify the name");
                }
                return null;
            }

            return SUPPORTED_ENCODERS.stream()
                .collect(Collectors.toMap(Encoder::getName, Encoder::getMethodComponent))
                .get(vResolved.getName().get())
                .resolveKNNIndexContext(v, context);
        }, v -> {
            if (v == null) {
                return null;
            }

            if (v.getName().isEmpty() && v.getParameters().isPresent()) {
                return ValidationUtil.chainValidationErrors(null, "Invalid configuration. Need to specify the name");
            }

            if (v.getName().isEmpty()) {
                return null;
            }

            if (SUPPORTED_ENCODERS.stream().map(Encoder::getName).collect(Collectors.toSet()).contains(v.getName().get()) == false) {
                return ValidationUtil.chainValidationErrors(null, "Invalid confidence interval. IMPROVE");
            }
            return null;
        }, SUPPORTED_ENCODERS.stream().collect(Collectors.toMap(Encoder::getName, Encoder::getMethodComponent)));
    }

    private static MethodComponentContext getDefaultEncoderFromCompression(
        CompressionConfig compressionConfig,
        WorkloadModeConfig workloadModeConfig
    ) {
        if (compressionConfig == CompressionConfig.NOT_CONFIGURED) {
            return getDefaultEncoderContextFromMode(workloadModeConfig);
        }

        if (compressionConfig == CompressionConfig.x32) {
            return DEFAULT_32x_ENCODER_CONTEXT;
        }

        if (compressionConfig == CompressionConfig.x16) {
            return DEFAULT_16x_ENCODER_CONTEXT;
        }

        if (compressionConfig == CompressionConfig.x8) {
            return DEFAULT_8x_ENCODER_CONTEXT;
        }

        return DEFAULT_ENCODER_CONTEXT;
    }

    private static MethodComponentContext getDefaultEncoderContextFromMode(WorkloadModeConfig workloadModeConfig) {
        if (workloadModeConfig == WorkloadModeConfig.ON_DISK) {
            return DEFAULT_32x_ENCODER_CONTEXT;
        }
        return DEFAULT_ENCODER_CONTEXT;
    }
}
