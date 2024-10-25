/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Class is used to iterate over a parents children ids.
 */
@AllArgsConstructor
public class ParentChildIterator {
    private final List<Integer> parentDocIds;
    private final List<Integer> childDocIds;

    public int numChildren(int parent) {
        int numChildren = 0;
        int child = firstChild(parent);
        while (child != -1 && child < parent) {
            numChildren += 1;
            child = nextChild(child, parent);
        }
        return numChildren;
    }

    public int firstChild(int parentOfChildrenDocId) {
        int parentBefore = -1;
        for (int parentDocId : parentDocIds) {
            if (parentDocId < parentOfChildrenDocId) {
                parentBefore = parentDocId;
            } else {
                break;
            }
        }

        for (int childDocId : childDocIds) {
            if (childDocId > parentBefore && childDocId < parentOfChildrenDocId) {
                return childDocId;
            }
        }

        return -1;
    }

    public int nextChild(int currentChild, int parent) {
        for (int childDocId : childDocIds) {
            if (childDocId <= currentChild) {
                continue;
            }

            if (childDocId < parent) {
                return childDocId;
            }
            break;
        }
        return -1;
    }
}
