/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.derivedsource;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.opensearch.knn.index.codec.derivedsource.ParentChildHelper.getParentField;

/**
 * This class provides an iterator over children of a parent document id. This is used by the
 * {@link NestedPerFieldDerivedVectorInjector} to inject the children of a given parent document id into the source of
 * a document.
 */
@AllArgsConstructor
public class ParentChildIterator {
    private final List<Integer> parentDocIds;
    private final List<Integer> childDocIds;

    // We do not really
    public static ParentChildIterator getParentChildIterator(
        FieldInfo childFieldInfo,
        SegmentReadState segmentReadState,
        DocValuesProducer docValuesProducer,
        FieldsProducer fieldsProducer
    ) throws IOException {
        // In OpenSearch, nested documents are indexed just as any documents, except that they contain a terms field,
        // _nested_path, that resolves to the parent field name. The logic for this is encapsulated in the
        // NestedPathFieldMapper,
        // https://github.com/opensearch-project/OpenSearch/blob/2.18.0/server/src/main/java/org/opensearch/index/mapper/NestedPathFieldMapper.java#L76-L78
        // and the DocumentParser,
        // https://github.com/opensearch-project/OpenSearch/blob/2.18.0/server/src/main/java/org/opensearch/index/mapper/DocumentParser.java#L515-L519.
        // So, to identify parents and children, you simply check if a document contains the path to the parent field.
        //
        // There are some nice utilities in OpenSearch that allow us to easily query for this information. However, in
        // our derived source we are operating at the lucene extension level, which does not have access to the
        // convenient utilities of OpenSearch. This is because those utilities are built on top of the constructs we are
        // modifying, so they would introduce circular dependencies if we tried to add them.
        //
        // Instead, we do things by hand. We extract the parent field from the child field. then
        String childField = childFieldInfo.name;
        String parentField = getParentField(childField);
        if (parentField == null) {
            return null;
        }

        // TODO: There should be a more elegant way to handle this
        // From the segmentReadState, we get the fieldInfo for the "_primary_term". The primary_term field is only set
        // for non-nested docs, so can identify root level parents. For reference:
        // 1.
        // https://github.com/opensearch-project/OpenSearch/blob/2.18.0/server/src/main/java/org/opensearch/search/fetch/subphase/SeqNoPrimaryTermPhase.java#L72
        // 2.
        // https://github.com/opensearch-project/OpenSearch/blob/3032bef54d502836789ea438f464ae0b1ba978b2/server/src/main/java/org/opensearch/index/mapper/SeqNoFieldMapper.java#L206-L230
        FieldInfo seqTermsFieldInfo = segmentReadState.fieldInfos.fieldInfo("_primary_term");
        List<Integer> parentIds = new ArrayList<>();
        NumericDocValues numericDocValues = docValuesProducer.getNumeric(seqTermsFieldInfo);
        while (numericDocValues.nextDoc() != NO_MORE_DOCS) {
            parentIds.add(numericDocValues.docID());
        }

        // Now that we have the parent ids, from the fieldsProducer, we get the terms for the "_nested_path". All d
        // documents that have the "_nested_path" set to the parent field are identified as children.
        List<Integer> childIds = new ArrayList<>();
        Terms terms = fieldsProducer.terms("_nested_path");
        TermsEnum nestedFieldsTerms = terms.iterator();
        BytesRef childPathRef = new BytesRef(parentField);
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

    /**
     * Given a parent id, return the number of children of that parent id.
     *
     * @param parent parent id
     * @return The number of children of the parent id
     */
    public int numChildren(int parent) {
        int numChildren = 0;
        int child = firstChild(parent);
        while (child != -1 && child < parent) {
            numChildren += 1;
            child = nextChild(child, parent);
        }
        return numChildren;
    }

    /**
     * Given a parent id, return the id of the first child of that parent id.
     *
     * @param parent parent id
     * @return The first child of the parent id or -1 if there are no children
     */
    public int firstChild(int parent) {
        int parentBefore = -1;
        for (int parentDocId : parentDocIds) {
            if (parentDocId < parent) {
                parentBefore = parentDocId;
            } else {
                break;
            }
        }

        for (int childDocId : childDocIds) {
            if (childDocId > parentBefore && childDocId < parent) {
                return childDocId;
            }
        }

        return -1;
    }

    /**
     * Give a child and a parent id, return the id of the next child of that parent id.
     *
     * @param currentChild The current child id
     * @param parent       The parent id
     * @return The next child of the parent id or -1 if there is no next child
     */
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
