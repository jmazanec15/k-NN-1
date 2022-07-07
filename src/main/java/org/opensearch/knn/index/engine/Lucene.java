/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.index.engine;

import com.google.common.collect.ImmutableMap;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.KNNMethod;
import org.opensearch.knn.index.KNNMethodContext;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.MethodComponent;
import org.opensearch.knn.index.Parameter;
import org.opensearch.knn.index.SpaceType;

import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;

class Lucene extends JVMLibrary {

    final static Map<String, KNNMethod> METHODS = ImmutableMap.of(
        METHOD_HNSW,
        KNNMethod.Builder.builder(
            MethodComponent.Builder.builder(METHOD_HNSW)
                .addParameter(
                    METHOD_PARAMETER_M,
                    new Parameter.IntegerParameter(METHOD_PARAMETER_M, KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M, v -> v > 0)
                )
                .addParameter(
                    METHOD_PARAMETER_EF_CONSTRUCTION,
                    new Parameter.IntegerParameter(
                        METHOD_PARAMETER_EF_CONSTRUCTION,
                        KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION,
                        v -> v > 0
                    )
                )
                .build()
        ).addSpaces(SpaceType.L2, SpaceType.COSINESIMIL).build()
    );

    final static Lucene INSTANCE = new Lucene(METHODS);

    Map<String, KNNMethod> methods;

    private Lucene(Map<String, KNNMethod> methods) {
        this.methods = methods;
    }

    @Override
    public KNNMethod getMethod(String methodName) {
        KNNMethod method = methods.get(methodName);
        if (method != null) {
            return method;
        }
        throw new IllegalArgumentException("Invalid method name: " + methodName);
    }

    @Override
    public ValidationException validateMethod(KNNMethodContext knnMethodContext) {
        String methodName = knnMethodContext.getMethodComponent().getName();
        return getMethod(methodName).validate(knnMethodContext);
    }

    @Override
    public boolean isTrainingRequired(KNNMethodContext knnMethodContext) {
        // TODO: In future, we can extend this
        return false;
    }
}
