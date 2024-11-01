/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

@Log4j2
@AllArgsConstructor
public class ParentChildHelper {

    private final FieldsProducer fieldsProducer;
    private final DocValuesProducer docValuesProducer;

    public ParentChildIterator getParentChildIterator(String childField, FieldInfo seqTermsFieldInfo) throws IOException {
        String parentField = getParentField(childField);

        if (parentField == null) {
            return null;
        }

        List<Integer> parentIds = new ArrayList<>();
        NumericDocValues numericDocValues = docValuesProducer.getNumeric(seqTermsFieldInfo);
        while (numericDocValues.nextDoc() != NO_MORE_DOCS) {
            parentIds.add(numericDocValues.docID());
        }

        BytesRef childPathRef = new BytesRef(parentField);
        List<Integer> childIds = new ArrayList<>();

        Terms terms = fieldsProducer.terms("_nested_path");
        TermsEnum nestedFieldsTerms = terms.iterator();
        while (nestedFieldsTerms.next() != null) {
            BytesRef currentTerm = nestedFieldsTerms.term();
            if (currentTerm.bytesEquals(childPathRef)) {
                PostingsEnum postingsEnum = nestedFieldsTerms.postings(null);
                while (postingsEnum.nextDoc() != NO_MORE_DOCS) {
                    if (postingsEnum.freq() > 0) {
                        childIds.add(postingsEnum.docID());
                    }
                }
            }
        }

        return new ParentChildIterator(parentIds, childIds);
    }

    public static String getParentField(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }

    public static String getChildField(String field) {
        int lastDot = field.lastIndexOf('.');
        return field.substring(lastDot + 1);
    }

    @AllArgsConstructor
    public static class ParentChildIterator {
        private final List<Integer> parentDocIds;
        private final List<Integer> childDocIds;

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
}
