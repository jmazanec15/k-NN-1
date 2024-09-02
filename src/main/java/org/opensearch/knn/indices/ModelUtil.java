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

package org.opensearch.knn.indices;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang.StringUtils;

import java.util.Locale;

/**
 * A utility class for models.
 */
@UtilityClass
public class ModelUtil {

    public static void blockCommasInModelDescription(String description) {
        if (description.contains(",")) {
            throw new IllegalArgumentException("Model description cannot contain any commas: ','");
        }
    }

    public static boolean isModelPresent(ModelMetadata modelMetadata) {
        return modelMetadata != null;
    }

    public static boolean isModelCreated(ModelMetadata modelMetadata) {
        if (!isModelPresent(modelMetadata)) {
            return false;
        }
        return modelMetadata.getState().equals(ModelState.CREATED);
    }

    /**
     * Gets Model Metadata from a given model id.
     *
     * @param modelId {@link String}
     * @return {@link ModelMetadata} or null if modelId is null or empty
     */
    public static ModelMetadata getModelMetadata(final String modelId) {
        if (StringUtils.isEmpty(modelId)) {
            return null;
        }
        // TODO: We need to initialize this class with ModelDao and get modelMetadata from there.
        final Model model = getModel(modelId);
        if (model == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Model ID '%s' does not exist.", modelId));
        }
        final ModelMetadata modelMetadata = model.getModelMetadata();
        if (isModelCreated(modelMetadata) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Model ID '%s' is not created.", modelId));
        }
        return modelMetadata;
    }

    /**
     * Gets the model from the cache
     *
     * @param modelId {@link String}
     * @return {@link Model} or null if modelId is null or empty
     */
    public static Model getModel(final String modelId) {
        if (StringUtils.isEmpty(modelId)) {
            return null;
        }
        return ModelCache.getInstance().get(modelId);
    }
}
