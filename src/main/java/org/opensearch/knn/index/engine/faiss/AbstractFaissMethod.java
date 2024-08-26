/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.faiss;

import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.AbstractKNNMethod;
import org.opensearch.knn.index.engine.KNNIndexContext;
import org.opensearch.knn.index.engine.KNNLibrarySearchContext;
import org.opensearch.knn.index.engine.MethodComponent;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;

import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.INDEX_DESCRIPTION_PARAMETER;

public abstract class AbstractFaissMethod extends AbstractKNNMethod {

    /**
     * Constructor for the AbstractFaissMethod class.
     *
     * @param methodComponent        The method component used to create the method
     * @param spaces                 The set of spaces supported by the method
     * @param knnLibrarySearchContext The KNN library search context
     */
    public AbstractFaissMethod(MethodComponent methodComponent, Set<SpaceType> spaces, KNNLibrarySearchContext knnLibrarySearchContext) {
        super(methodComponent, spaces, knnLibrarySearchContext);
    }

    // For faiss, we need to update the index description. For this, it will require getting parameters that have been
    // added to the map and putting them into the index description
    @Override
    protected ValidationException postResolveProcess(KNNIndexContext knnIndexContext) {
        String initialIndexDescription = "";
        if (knnIndexContext.getVectorDataType() == VectorDataType.BINARY
            || knnIndexContext.getQuantizationConfig() != QuantizationConfig.EMPTY) {
            initialIndexDescription = "B";
        }
        knnIndexContext.getLibraryParameters().put(INDEX_DESCRIPTION_PARAMETER, initialIndexDescription);
        return methodComponent.postResolveProcess(knnIndexContext, knnIndexContext.getLibraryParameters());
    }
}
