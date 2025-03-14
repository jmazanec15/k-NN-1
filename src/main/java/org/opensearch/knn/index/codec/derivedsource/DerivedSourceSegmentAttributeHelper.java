/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;

import java.util.Arrays;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class for adding information into the segment attributes
 */
public class DerivedSourceSegmentAttributeHelper {

    public static final String DERIVED_SOURCE_FIELD = "derived_vector_fields";
    public static final String NESTED_LINEAGE = "derived_vector_fields_nested_lineage";

    /**
     * From segmentInfo, parse the derived_vector_fields
     *
     * @param segmentInfo {@link SegmentInfo}
     * @param fieldInfos {@link FieldInfo}
     * @return List of fields that derived source is enabled for. Potentially null if no fields
     */
    public static List<String> parseDerivedVectorFields(SegmentInfo segmentInfo, FieldInfos fieldInfos) {
        if (segmentInfo == null) {
            return null;
        }
        String derivedVectorFields = segmentInfo.getAttribute(DERIVED_SOURCE_FIELD);
        if (derivedVectorFields == null || derivedVectorFields.isEmpty()) {
            return null;
        }
        return Arrays.stream(derivedVectorFields.split(","))
            .collect(Collectors.toList());
    }

    /**
     * Parses the nested lineage map from the segment info
     *
     * @param vectorFields Fields of vectors
     * @param segmentInfo {@link SegmentInfo}
     * @return Mapping between derived source fields and their nested lineage
     */
    public static Map<String, Boolean> parseNestedMap(List<String> vectorFields, SegmentInfo segmentInfo) {
        if (segmentInfo == null) {
            return null;
        }
        String nestedLineage = segmentInfo.getAttribute(NESTED_LINEAGE);
        if (nestedLineage == null || nestedLineage.isEmpty()) {
            return null;
        }

        Map<String, Boolean> nestedMap = new HashMap<>();
        String[] nested = nestedLineage.split(",", -1);
        for (int i = 0; i < nested.length; i++) {
            nestedMap.put(vectorFields.get(i), Boolean.valueOf(nested[i]));
        }
        return nestedMap;
    }

    /**
     * Adds {@link SegmentInfo} attribute for vectorFieldTypes
     *
     * @param segmentInfo {@link SegmentInfo}
     * @param vectorFieldTypes List of vector field names
     */
    public static void addDerivedVectorFieldsSegmentInfoAttribute(SegmentInfo segmentInfo, List<String> vectorFieldTypes) {
        segmentInfo.putAttribute(DERIVED_SOURCE_FIELD, String.join(",", vectorFieldTypes));
    }

    /**
     * Adds {@link SegmentInfo} attribute for nested lineage of all derived source fields
     *
     * @param segmentInfo {@link SegmentInfo}
     * @param isNestedList List of lists of parent and grandparent fields. Order should match that of the
     *                      ector field types
     */
    public static void addNestedLineageSegmentInfoAttribute(SegmentInfo segmentInfo, List<Boolean> isNestedList) {
        segmentInfo.putAttribute(
            NESTED_LINEAGE,
            isNestedList.stream().map(Object::toString).collect(Collectors.joining(","))
        );
    }
}
