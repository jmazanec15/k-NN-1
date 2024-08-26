/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.knn.index.VectorDataType;

/**
 * Class provides the parameters that the user explicitly provided for configuring their k-NN index. All valus
 * can potentially be null and should not be used outside of configuration for {@link KNNIndexContext}
 */
@AllArgsConstructor
@Getter
public final class UserProvidedParameters {
    private final Integer dimension;
    private final VectorDataType vectorDataType;
    private final String modelId;
    private final String mode;
    private final String compressionLevel;
    private final KNNMethodContext knnMethodContext;
}
