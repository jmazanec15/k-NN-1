/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import com.google.common.collect.ImmutableMap;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.engine.validation.ParameterValidator;
import org.opensearch.knn.index.query.request.MethodParameter;

import java.util.Map;

public final class DefaultIVFSearchResolver extends FilterKNNLibraryIndexSearchResolver {

    private final Map<String, Parameter<?>> supportedMethodParameters = ImmutableMap.<String, Parameter<?>>builder()
        .put(MethodParameter.NPROBE.getName(), new Parameter.IntegerParameter(MethodParameter.NPROBE.getName(), (v, c) -> {
            throw new UnsupportedOperationException("Not supported");
        }, v -> null))
        .build();

    public DefaultIVFSearchResolver(KNNLibraryIndexSearchResolver delegate) {
        super(delegate);
    }

    @Override
    public Map<String, Object> resolveMethodParameters(QueryContext ctx, Map<String, Object> userParameters) {
        ValidationException validationException = ParameterValidator.validateParameters(supportedMethodParameters, userParameters);
        if (validationException != null) {
            throw validationException;
        }
        return userParameters;
    }
}
