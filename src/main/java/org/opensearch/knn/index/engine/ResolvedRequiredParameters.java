/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.Getter;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;

import java.util.Objects;
import java.util.Optional;

import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.createKNNMethodContextFromLegacy;

/**
 * Resolved parameters required for constructing a {@link KNNIndexContext}. If any of these parameters can be null,
 * then their getters need to be wrapped in an {@link java.util.Optional}
 */
public final class ResolvedRequiredParameters {
    @Getter
    private final VectorDataType vectorDataType;
    @Getter
    private final WorkloadModeConfig mode;
    @Getter
    private final SpaceType spaceType;
    @Getter
    private final KNNEngine knnEngine;
    @Getter
    private final CompressionConfig compressionConfig;
    @Getter
    private final Version createdVersion;
    @Getter
    private final int dimension;
    @Nullable
    private final KNNMethodContext knnMethodContext;

    /**
     *
     * @param originalParameters The original user provided parameters
     * @param settings Settings for the index; passing null will mean that it is not possible to resolve for the legacy
     * @param createdVersion version that this was created for
     */
    public ResolvedRequiredParameters(UserProvidedParameters originalParameters, Settings settings, Version createdVersion) {
        this.dimension = Objects.requireNonNull(originalParameters.getDimension(), "dimension must be set for ResolvedRequiredParameters");
        this.vectorDataType = Objects.requireNonNull(
            originalParameters.getVectorDataType() == null ? VectorDataType.DEFAULT : originalParameters.getVectorDataType(),
            "vectorDataType must be set for ResolvedRequiredParameters"
        );
        this.spaceType = Objects.requireNonNull(
            SpaceTypeResolver.resolveSpaceType(originalParameters.getKnnMethodContext(), this.vectorDataType),
            "spaceType must be set for ResolvedRequiredParameters"
        );
        this.mode = Objects.requireNonNull(
            resolveWorkloadModeConfig(originalParameters.getMode()),
            "mode must be set for ResolvedRequiredParameters"
        );
        this.compressionConfig = Objects.requireNonNull(
            CompressionConfig.fromString(originalParameters.getCompressionLevel()),
            "compressionConfig must be set for ResolvedRequiredParameters"
        );
        boolean isLegacy = computeIsLegacy(originalParameters.getKnnMethodContext(), mode, compressionConfig, vectorDataType, settings);
        this.knnMethodContext = isLegacy
            ? createKNNMethodContextFromLegacy(settings, createdVersion)
            : originalParameters.getKnnMethodContext();
        this.knnEngine = Objects.requireNonNull(
            KNNEngineResolver.resolveKNNEngine(knnMethodContext, vectorDataType, mode, compressionConfig),
            "knnEngine must be set for ResolvedRequiredParameters"
        );
        this.createdVersion = Objects.requireNonNull(createdVersion, "createdVersion must be set for ResolvedRequiredParameters");
    }

    public KNNIndexContext resolveKNNIndexContext(boolean shouldTrain) {
        KNNIndexContext knnIndexContext = new KNNIndexContext(this);
        ValidationException validationException = knnEngine.resolveKNNIndexContext(knnIndexContext, shouldTrain);
        if (validationException != null) {
            throw validationException;
        }
        return knnIndexContext;
    }

    /**
     *
     * @return Optional containing the knnMethodContext if it exists, otherwise an empty Optional
     */
    public Optional<KNNMethodContext> getKnnMethodContext() {
        return Optional.ofNullable(knnMethodContext);
    }

    private WorkloadModeConfig resolveWorkloadModeConfig(String userProvidedMode) {
        WorkloadModeConfig workloadModeConfig = WorkloadModeConfig.fromString(userProvidedMode);
        if (workloadModeConfig == WorkloadModeConfig.NOT_CONFIGURED) {
            return WorkloadModeConfig.DEFAULT;
        }
        return workloadModeConfig;
    }

    private boolean computeIsLegacy(
        KNNMethodContext originalKNNMethodContext,
        WorkloadModeConfig workloadModeConfig,
        CompressionConfig compressionConfig,
        VectorDataType vectorDataType,
        Settings settings
    ) {
        if (settings == null) {
            return false;
        }
        if (originalKNNMethodContext != null) {
            return false;
        }

        if (vectorDataType != VectorDataType.DEFAULT) {
            return false;
        }

        if (workloadModeConfig != WorkloadModeConfig.DEFAULT) {
            return false;
        }

        if (compressionConfig != CompressionConfig.DEFAULT && compressionConfig != CompressionConfig.NOT_CONFIGURED) {
            return false;
        }

        return true;
    }
}
