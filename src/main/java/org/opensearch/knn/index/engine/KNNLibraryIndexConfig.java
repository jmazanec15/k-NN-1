/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.Version;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;

/**
 * Resolved parameters required for constructing a {@link KNNLibraryIndexConfig}. If any of these parameters can be null,
 * then their getters need to be wrapped in an {@link java.util.Optional}
 */
@Getter
@AllArgsConstructor
public final class KNNLibraryIndexConfig {
    @NonNull
    private final VectorDataType vectorDataType;
    @NonNull
    private final SpaceType spaceType;
    @NonNull
    private final KNNEngine knnEngine;
    private final int dimension;
    @NonNull
    private final Version createdVersion;
    @NonNull
    private final MethodComponentContext methodComponentContext;
    @NonNull
    private final WorkloadModeConfig mode;
    @NonNull
    private final CompressionConfig compressionConfig;
    private final boolean shouldIndexConfigRequireTraining;
}
