/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.Version;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.mapper.PerDimensionProcessor;
import org.opensearch.knn.index.mapper.PerDimensionValidator;
import org.opensearch.knn.index.mapper.VectorValidator;

import java.util.Map;
import java.util.Objects;

/**
 * Class provides the context to build an index for ANN search. All configuration is resolved before c
 * construction and
 */
public final class KNNIndexContext {
    // TODO: Switch to builder pattern at some point
    @Getter
    private final ResolvedRequiredParameters resolvedRequiredParameters;

    public KNNIndexContext(ResolvedRequiredParameters resolvedRequiredParameters) {
        this.resolvedRequiredParameters = Objects.requireNonNull(
            resolvedRequiredParameters,
            "resolvedRequiredParameters must be set for KNNIndexContext"
        );
        this.estimatedIndexOverhead = 0;
        this.isTrainingRequired = false;
        this.quantizationConfig = QuantizationConfig.EMPTY;
    }

    /**
     * Library parameters define the generic map of parameters that are used to build the index for the library. While
     * a library ultimately decides what the structure of these parameters need to be, its typical (i.e. faiss) to
     * have the index configuration parameters in a nested parameters map.
     */
    @Setter
    @Getter
    private Map<String, Object> libraryParameters;
    @Setter
    @Getter
    private KNNLibrarySearchContext knnLibrarySearchContext;
    @Setter
    @Getter
    private QuantizationConfig quantizationConfig;
    @Setter
    @Getter
    private VectorValidator vectorValidator;
    @Setter
    @Getter
    private PerDimensionValidator perDimensionValidator;
    @Setter
    @Getter
    private PerDimensionProcessor perDimensionProcessor;

    @Getter
    private Integer estimatedIndexOverhead;
    @Getter
    private boolean isTrainingRequired;

    public void increaseOverheadEstimate(int additionalOverhead) {
        this.estimatedIndexOverhead += additionalOverhead;
    }

    public void appendTrainingRequirement(boolean isTrainingRequired) {
        this.isTrainingRequired = this.isTrainingRequired || isTrainingRequired;
    }

    // TODO: Baseline getters
    public KNNEngine getKNNEngine() {
        return resolvedRequiredParameters.getKnnEngine();
    }

    public SpaceType getSpaceType() {
        return resolvedRequiredParameters.getSpaceType();
    }

    public VectorDataType getVectorDataType() {
        return resolvedRequiredParameters.getVectorDataType();
    }

    public Version getCreatedVersion() {
        return resolvedRequiredParameters.getCreatedVersion();
    }

    public int getDimension() {
        return resolvedRequiredParameters.getDimension();
    }
}
