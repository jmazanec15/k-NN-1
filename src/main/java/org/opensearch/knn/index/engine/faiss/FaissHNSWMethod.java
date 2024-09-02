/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import com.google.common.collect.ImmutableSet;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.AbstractKNNMethod;
import org.opensearch.knn.index.engine.DefaultHnswSearchResolver;
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

import static org.opensearch.knn.common.KNNConstants.FAISS_HNSW_DESCRIPTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_ENCODER_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_SEARCH;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static org.opensearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
import static org.opensearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_SEARCH;
import static org.opensearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M;

/**
 * Faiss HNSW method implementation
 */
public class FaissHNSWMethod extends AbstractKNNMethod {

    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(
        VectorDataType.FLOAT,
        VectorDataType.BINARY,
        VectorDataType.BYTE
    );

    public final static List<SpaceType> SUPPORTED_SPACES = Arrays.asList(SpaceType.HAMMING, SpaceType.L2, SpaceType.INNER_PRODUCT);

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
        new FaissHNSWPQEncoder(),
        new QFrameBitEncoder()
    );

    /**
     * Constructor for FaissHNSWMethod
     *
     * @see AbstractKNNMethod
     */
    public FaissHNSWMethod() {
        super(initMethodComponent(), Set.copyOf(SUPPORTED_SPACES));
    }

    private static MethodComponent initMethodComponent() {
        return MethodComponent.Builder.builder(METHOD_HNSW)
            .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
            .addParameter(METHOD_PARAMETER_M, new Parameter.IntegerParameter(METHOD_PARAMETER_M, (v, context) -> {
                Integer vResolved = v;
                if (vResolved == null) {
                    vResolved = INDEX_KNN_DEFAULT_ALGO_PARAM_M;
                }
                context.getLibraryParameters().put(METHOD_PARAMETER_M, vResolved);
            }, v -> {
                if (v == null) {
                    return null;
                }
                return ValidationUtil.chainValidationErrors(null, v > 0 ? null : "UPDATE ME");
            }))
            .addParameter(
                METHOD_PARAMETER_EF_CONSTRUCTION,
                new Parameter.IntegerParameter(METHOD_PARAMETER_EF_CONSTRUCTION, (v, context) -> {
                    Integer vResolved = v;
                    if (vResolved == null) {
                        vResolved = INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
                    }
                    context.getLibraryParameters().put(METHOD_PARAMETER_EF_CONSTRUCTION, vResolved);
                }, v -> {
                    if (v == null) {
                        return null;
                    }
                    return ValidationUtil.chainValidationErrors(null, v > 0 ? null : "UPDATE ME");
                })
            )
            .addParameter(METHOD_PARAMETER_EF_SEARCH, new Parameter.IntegerParameter(METHOD_PARAMETER_EF_SEARCH, (v, context) -> {
                Integer vResolved = v;
                if (vResolved == null) {
                    vResolved = INDEX_KNN_DEFAULT_ALGO_PARAM_EF_SEARCH;
                }
                context.getLibraryParameters().put(METHOD_PARAMETER_EF_SEARCH, vResolved);
            }, v -> {
                if (v == null) {
                    return null;
                }
                return ValidationUtil.chainValidationErrors(null, v > 0 ? null : "UPDATE ME");
            }))
            .addParameter(METHOD_ENCODER_PARAMETER, initEncoderParameter())
            .setPostResolveProcessor(((methodComponent, builder) -> {
                ValidationException validationException = IndexDescriptionPostResolveProcessor.builder(
                    FAISS_HNSW_DESCRIPTION,
                    methodComponent,
                    builder
                ).setTopLevel(true).addParameter(METHOD_PARAMETER_M, "", "").addParameter(METHOD_ENCODER_PARAMETER, "", "").build();
                if (validationException != null) {
                    throw validationException;
                }
                builder.knnLibraryIndexSearchResolver(new DefaultHnswSearchResolver(builder.getKnnLibraryIndexSearchResolver()));
            }))
            .build();
    }

    private static Parameter.MethodComponentContextParameter initEncoderParameter() {
        return new Parameter.MethodComponentContextParameter(METHOD_ENCODER_PARAMETER, (v, context) -> {
            MethodComponentContext vResolved = v;
            if (vResolved == null) {
                vResolved = getDefaultEncoderFromCompression(
                    context.getKnnLibraryIndexConfig().getCompressionConfig(),
                    context.getKnnLibraryIndexConfig().getMode()
                );
            }

            if (vResolved.getName().isEmpty()) {
                if (vResolved.getParameters().isPresent()) {
                    context.addValidationErrorMessage("Invalid configuration. Need to specify the name", true);
                }
            }

            SUPPORTED_ENCODERS.stream()
                .collect(Collectors.toMap(Encoder::getName, Encoder::getMethodComponent))
                .get(vResolved.getName().get())
                .resolve(v, context);
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
