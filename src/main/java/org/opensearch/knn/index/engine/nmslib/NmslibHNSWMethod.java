/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.nmslib;

import com.google.common.collect.ImmutableSet;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.AbstractKNNMethod;
import org.opensearch.knn.index.engine.DefaultHnswSearchContext;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;

/**
 * Nmslib's HNSW implementation
 */
public class NmslibHNSWMethod extends AbstractKNNMethod {

    private static final Set<VectorDataType> SUPPORTED_DATA_TYPES = ImmutableSet.of(VectorDataType.FLOAT);

    public final static List<SpaceType> SUPPORTED_SPACES = Arrays.asList(
        SpaceType.L2,
        SpaceType.L1,
        SpaceType.LINF,
        SpaceType.COSINESIMIL,
        SpaceType.INNER_PRODUCT
    );

    /**
     * Constructor. Builds the method with the default parameters and supported spaces.
     * @see AbstractKNNMethod
     */
    public NmslibHNSWMethod() {
        super(initMethodComponent(), Set.copyOf(SUPPORTED_SPACES), new DefaultHnswSearchContext());
    }

    private static MethodComponent initMethodComponent() {
        return MethodComponent.Builder.builder(METHOD_HNSW)
            .addSupportedDataTypes(SUPPORTED_DATA_TYPES)
            .addParameter(METHOD_PARAMETER_M, new Parameter.IntegerParameter(METHOD_PARAMETER_M, (v, context) -> {
                Integer vResolved = v;
                if (v == null) {
                    vResolved = KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M;
                }
                context.getLibraryParameters().put(METHOD_PARAMETER_M, vResolved);
                return null;
            }, (v) -> {
                if (v == null) {
                    return null;
                }
                if (v > 0) {
                    return null;
                }
                String message = String.format(
                    Locale.ROOT,
                    "Invalid value for parameter '%s'. Value must be greater than 0",
                    METHOD_PARAMETER_M
                );
                ValidationException validationException = new ValidationException();
                validationException.addValidationError(message);
                return validationException;
            }))
            .addParameter(
                METHOD_PARAMETER_EF_CONSTRUCTION,
                new Parameter.IntegerParameter(METHOD_PARAMETER_EF_CONSTRUCTION, (v, context) -> {
                    Integer vResolved = v;
                    if (v == null) {
                        vResolved = KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
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
                    String message = String.format(
                        Locale.ROOT,
                        "Invalid value for parameter '%s'. Value must be greater than 0",
                        METHOD_PARAMETER_EF_CONSTRUCTION
                    );
                    ValidationException validationException = new ValidationException();
                    validationException.addValidationError(message);
                    return validationException;
                })
            )
            .build();
    }
}
