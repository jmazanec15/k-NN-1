/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.index.engine;

import org.opensearch.knn.index.KNNMethodContext;
import org.opensearch.knn.index.SpaceType;

import java.util.Map;

abstract class JVMLibrary implements KNNLibrary {
    @Override
    public String getLatestBuildVersion() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public String getLatestLibVersion() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public String getExtension() {
        // TODO: Should this default to whatever lucene is? I dont think so
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public String getCompoundExtension() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public float score(float rawScore, SpaceType spaceType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int estimateOverheadInKB(KNNMethodContext knnMethodContext, int dimension) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<String, Object> getMethodAsMap(KNNMethodContext knnMethodContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Boolean isInitialized() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void setInitialized(Boolean isInitialized) {
        throw new UnsupportedOperationException("Not supported");
    }
}
