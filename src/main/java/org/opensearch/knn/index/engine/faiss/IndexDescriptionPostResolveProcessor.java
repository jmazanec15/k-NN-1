/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.engine.KNNIndexContext;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.Parameter;

import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.INDEX_DESCRIPTION_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.NAME;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;

/**
 * MethodAsMap builder is used to create the map that will be passed to the jni to create the faiss index.
 * Faiss's index factory takes an "index description" that it uses to build the index. In this description,
 * some parameters of the index can be configured; others need to be manually set. MethodMap builder creates
 * the index description from a set of parameters and removes them from the map. On build, it sets the index
 * description in the map and returns the processed map
 */
@AllArgsConstructor
@Getter
class IndexDescriptionPostResolveProcessor {
    String indexDescription;
    MethodComponent methodComponent;
    Map<String, Object> methodAsMap;
    KNNIndexContext knnIndexContext;

    /**
     * Add a parameter that will be used in the index description for the given method component
     *
     * @param parameterName name of the parameter
     * @param prefix to append to the index description before the parameter
     * @param suffix to append to the index description after the parameter
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    IndexDescriptionPostResolveProcessor addParameter(String parameterName, String prefix, String suffix) {
        indexDescription += prefix;
        Map<String, Object> methodParameters = (Map<String, Object>) methodAsMap.get(PARAMETERS);
        Parameter<?> parameter = methodComponent.getParameters().get(parameterName);

        // Recursion is needed if the parameter is a method component context itself.
        if (parameter instanceof Parameter.MethodComponentContextParameter) {
            Map<String, Object> subMethodParameters = (Map<String, Object>) methodParameters.get(parameterName);
            MethodComponent subMethodComponent = ((Parameter.MethodComponentContextParameter) parameter).getMethodComponent(
                (String) subMethodParameters.get(NAME)
            );
            knnIndexContext.getLibraryParameters().put(KNNConstants.INDEX_DESCRIPTION_PARAMETER, indexDescription);
            ValidationException validationException = subMethodComponent.postResolveProcess(knnIndexContext, subMethodParameters);
            if (validationException != null) {
                throw validationException;
            }
            if (subMethodParameters == null
                || subMethodParameters.isEmpty()
                || subMethodParameters.get(PARAMETERS) == null
                || ((Map<String, Object>) subMethodParameters.get(PARAMETERS)).isEmpty()) {
                methodParameters.remove(parameterName);
            }
            indexDescription = (String) knnIndexContext.getLibraryParameters().get(INDEX_DESCRIPTION_PARAMETER);
        } else {
            // Just add the value to the method description and remove from map
            indexDescription += methodParameters.get(parameterName);
            methodParameters.remove(parameterName);
        }

        indexDescription += suffix;
        knnIndexContext.getLibraryParameters().put(KNNConstants.INDEX_DESCRIPTION_PARAMETER, indexDescription);
        return this;
    }

    /**
     * Build
     *
     * @return Method as a map
     */
    ValidationException build() {
        return null;
    }

    static IndexDescriptionPostResolveProcessor builder(
        String baseDescription,
        MethodComponent methodComponent,
        KNNIndexContext knnIndexContext,
        Map<String, Object> contextLibraryParams
    ) {
        String initialDescription = (String) knnIndexContext.getLibraryParameters().get(KNNConstants.INDEX_DESCRIPTION_PARAMETER);
        if (initialDescription == null) {
            initialDescription = "";
        }
        initialDescription += baseDescription;
        knnIndexContext.getLibraryParameters().put(KNNConstants.INDEX_DESCRIPTION_PARAMETER, initialDescription);
        return new IndexDescriptionPostResolveProcessor(baseDescription, methodComponent, contextLibraryParams, knnIndexContext);
    }
}
