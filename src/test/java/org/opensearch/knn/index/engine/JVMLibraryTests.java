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

import org.opensearch.common.ValidationException;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.KNNMethod;
import org.opensearch.knn.index.KNNMethodContext;
import org.opensearch.knn.index.SpaceType;

public class JVMLibraryTests extends KNNTestCase {

    private final static TestJVMLibrary TEST_JVM_LIBRARY = new TestJVMLibrary();

    public void testGetLatestBuildVersion() {
        expectThrows(UnsupportedOperationException.class, TEST_JVM_LIBRARY::getLatestBuildVersion);
    }

    public void testGetLatestLibVersion() {
        expectThrows(UnsupportedOperationException.class, TEST_JVM_LIBRARY::getLatestLibVersion);
    }

    public void testGetExtension() {
        expectThrows(UnsupportedOperationException.class, TEST_JVM_LIBRARY::getExtension);
    }

    public void testGetCompoundExtension() {
        expectThrows(UnsupportedOperationException.class, TEST_JVM_LIBRARY::getCompoundExtension);
    }

    public void testScore() {
        expectThrows(UnsupportedOperationException.class, () -> TEST_JVM_LIBRARY.score(0.0f, SpaceType.DEFAULT));
    }

    public void testEstimateOverheadInKB() {
        expectThrows(UnsupportedOperationException.class, () -> TEST_JVM_LIBRARY.estimateOverheadInKB(null, 0));
    }

    public void testGetMethodAsMap() {
        expectThrows(UnsupportedOperationException.class, () -> TEST_JVM_LIBRARY.getMethodAsMap(null));
    }

    public void testIsInitialized() {
        expectThrows(UnsupportedOperationException.class, TEST_JVM_LIBRARY::isInitialized);
    }

    public void testSetInitialized() {
        expectThrows(UnsupportedOperationException.class, () -> TEST_JVM_LIBRARY.setInitialized(false));
    }

    static class TestJVMLibrary extends JVMLibrary {

        TestJVMLibrary() {}

        @Override
        public KNNMethod getMethod(String methodName) {
            return null;
        }

        @Override
        public ValidationException validateMethod(KNNMethodContext knnMethodContext) {
            return null;
        }

        @Override
        public boolean isTrainingRequired(KNNMethodContext knnMethodContext) {
            return false;
        }
    }
}
