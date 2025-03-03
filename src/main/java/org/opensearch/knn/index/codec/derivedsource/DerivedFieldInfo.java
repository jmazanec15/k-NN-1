/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.index.FieldInfo;

/**
 * Light wrapper around {@link FieldInfo} that indicates whether the field is nested or not.
 */
@Builder
@AllArgsConstructor
@Getter
public class DerivedFieldInfo {
    private final FieldInfo fieldInfo;
    private final boolean isNested;

    /**
     * @return name of the field
     */
    public String name() {
        return fieldInfo.name;
    }

    public FieldInfo fieldInfo() {
        return fieldInfo;
    }
}
