/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.lucene;

import com.google.common.collect.ImmutableSet;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.AbstractKNNMethod;
import org.opensearch.knn.index.engine.Encoder;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.knn.common.KNNConstants.METHOD_ENCODER_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static org.opensearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
import static org.opensearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M;

/**
 * Lucene HNSW implementation
 */
public class LuceneHNSWMethod extends AbstractKNNMethod {

    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT, VectorDataType.BYTE);

    public final static List<SpaceType> SUPPORTED_SPACES = Arrays.asList(SpaceType.L2, SpaceType.COSINESIMIL, SpaceType.INNER_PRODUCT);

    private final static MethodComponentContext DEFAULT_ENCODER_CONTEXT = null;
    private final static List<Encoder> SUPPORTED_ENCODERS = List.of(new LuceneSQEncoder());

    /**
     * Constructor for LuceneHNSWMethod
     *
     * @see AbstractKNNMethod
     */
    public LuceneHNSWMethod() {
        super(initMethodComponent(), Set.copyOf(SUPPORTED_SPACES), new LuceneHNSWSearchContext());
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
                return null;
            }, v -> {
                if (v == null) {
                    return null;
                }
                if (v > 0) {
                    return null;
                }
                return ValidationUtil.chainValidationErrors(null, "Invalid confidence interval. IMPROVE");
            }))
            .addParameter(
                METHOD_PARAMETER_EF_CONSTRUCTION,
                new Parameter.IntegerParameter(METHOD_PARAMETER_EF_CONSTRUCTION, (v, context) -> {
                    Integer vResolved = v;
                    if (vResolved == null) {
                        vResolved = INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
                    }
                    context.getLibraryParameters().put(METHOD_PARAMETER_EF_CONSTRUCTION, vResolved);
                    return null;
                }, v -> {
                    if (v == null) {
                        return null;
                    }
                    if (v > 0) {
                        return null;
                    }
                    return ValidationUtil.chainValidationErrors(null, "Invalid confidence interval. IMPROVE");
                })
            )
            .addParameter(METHOD_ENCODER_PARAMETER, initEncoderParameter())
            .build();
    }

    private static Parameter.MethodComponentContextParameter initEncoderParameter() {
        return new Parameter.MethodComponentContextParameter(METHOD_ENCODER_PARAMETER, (v, context) -> {
            if (v == null) {
                return null;
            }

            if (v.getName().isEmpty()) {
                if (v.getParameters().isPresent()) {
                    return ValidationUtil.chainValidationErrors(null, "Invalid configuration. Need to specify the name");
                }
                return null;
            }

            return SUPPORTED_ENCODERS.stream()
                .collect(Collectors.toMap(Encoder::getName, Encoder::getMethodComponent))
                .get(v.getName().get())
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
}
