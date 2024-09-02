/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.opensearch.common.Nullable;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;

import java.util.Map;

@Value
@Builder
@ToString
public class BuildIndexParams {
    String fieldName;
    KNNEngine knnEngine;
    String indexPath;
    /**
     * Vector data type represents the type used to build the library index. If something like binary quantization is
     * done, then this will be different from the vector data type the user provides
     */
    VectorDataType vectorDataType;
    Map<String, Object> parameters;
    /**
     * An optional quantization state that contains required information for quantization
     */
    @Nullable
    QuantizationState quantizationState;
}
