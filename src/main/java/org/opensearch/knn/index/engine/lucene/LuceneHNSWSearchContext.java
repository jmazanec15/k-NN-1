/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.lucene;

import com.google.common.collect.ImmutableMap;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.engine.KNNLibrarySearchContext;
import org.opensearch.knn.index.engine.Parameter;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.engine.validation.ParameterValidator;
import org.opensearch.knn.index.query.request.MethodParameter;
import org.opensearch.knn.index.query.rescore.RescoreContext;

import java.util.Map;

public class LuceneHNSWSearchContext implements KNNLibrarySearchContext {

    private final Map<String, Parameter<?>> supportedMethodParameters = ImmutableMap.<String, Parameter<?>>builder()
        .put(MethodParameter.EF_SEARCH.getName(), new Parameter.IntegerParameter(MethodParameter.EF_SEARCH.getName(), (v, c) -> {
            throw new UnsupportedOperationException("Not supported");
        }, v -> null))
        .build();

    @Override
    public Map<String, Object> processMethodParameters(QueryContext ctx, Map<String, Object> parameters) {
        if (ctx.getQueryType().isRadialSearch() && parameters.isEmpty() == false) {
            // return empty map if radial search is true
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("Radial search does not support any parameters");
            throw validationException;
        }

        ValidationException validationException = ParameterValidator.validateParameters(supportedMethodParameters, parameters);
        if (validationException != null) {
            throw validationException;
        }

        return parameters;
    }

    @Override
    public RescoreContext getDefaultRescoreContext(QueryContext ctx) {
        return null;
    }
}
