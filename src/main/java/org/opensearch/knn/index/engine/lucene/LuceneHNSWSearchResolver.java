/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.lucene;

import com.google.common.collect.ImmutableMap;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.engine.FilterKNNLibraryIndexSearchResolver;
import org.opensearch.knn.index.engine.KNNLibraryIndexSearchResolver;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.engine.validation.ParameterValidator;
import org.opensearch.knn.index.query.request.MethodParameter;

import java.util.Map;

public class LuceneHNSWSearchResolver extends FilterKNNLibraryIndexSearchResolver {

    private final Map<String, Parameter<?>> supportedMethodParameters = ImmutableMap.<String, Parameter<?>>builder()
        .put(MethodParameter.EF_SEARCH.getName(), new Parameter.IntegerParameter(MethodParameter.EF_SEARCH.getName(), (v, c) -> {
            throw new UnsupportedOperationException("Not supported");
        }, v -> null))
        .build();

    public LuceneHNSWSearchResolver(KNNLibraryIndexSearchResolver delegate) {
        super(delegate);
    }

    @Override
    public Map<String, Object> resolveMethodParameters(QueryContext ctx, Map<String, Object> userParameters) {
        if (ctx.getQueryType().isRadialSearch() && userParameters.isEmpty() == false) {
            // return empty map if radial search is true
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("Radial search does not support any parameters");
            throw validationException;
        }

        ValidationException validationException = ParameterValidator.validateParameters(supportedMethodParameters, userParameters);
        if (validationException != null) {
            throw validationException;
        }

        return userParameters;
    }
}
