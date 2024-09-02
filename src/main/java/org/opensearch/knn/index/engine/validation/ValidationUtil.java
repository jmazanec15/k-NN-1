/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.validation;

import org.opensearch.common.ValidationException;

public final class ValidationUtil {
    public static ValidationException chainValidationErrors(ValidationException input, String newExceptionError) {
        if (newExceptionError == null) {
            return input;
        }

        if (input == null) {
            input = new ValidationException();
        }

        input.addValidationError(newExceptionError);
        return input;
    }
}
