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
import org.opensearch.Version;
import org.opensearch.knn.index.engine.KNNIndexContext;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.engine.ResolvedRequiredParameters;
import org.opensearch.knn.index.engine.UserProvidedParameters;

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
     * @param modelId {@link String}
     * @return {@link ModelMetadata}
     */
    public static ModelMetadata getModelMetadata(final String modelId) {
        if (StringUtils.isEmpty(modelId)) {
            return null;
        }
        final Model model = ModelCache.getInstance().get(modelId);
        final ModelMetadata modelMetadata = model.getModelMetadata();
        if (isModelCreated(modelMetadata) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Model ID '%s' is not created.", modelId));
        }
        return modelMetadata;
    }

    /**
     * Wraps model metadata call to get the component context to return {@link KNNMethodContext}
     *
     * @param modelMetadata {@link ModelMetadata}
     * @return {@link KNNMethodContext} or null if method component context is empty
     */
    public static KNNMethodContext getMethodContextForModel(ModelMetadata modelMetadata) {
        MethodComponentContext methodComponentContext = modelMetadata.getMethodComponentContext();
        if (methodComponentContext == MethodComponentContext.EMPTY) {
            return null;
        }
        return new KNNMethodContext(modelMetadata.getKnnEngine(), modelMetadata.getSpaceType(), methodComponentContext);
    }

    public static KNNIndexContext getKnnMethodContextFromModelMetadata(String modelId, ModelMetadata modelMetadata) {
        MethodComponentContext methodComponentContext = modelMetadata.getMethodComponentContext();
        if (methodComponentContext == MethodComponentContext.EMPTY) {
            return null;
        }
        UserProvidedParameters userProvidedParameters = new UserProvidedParameters(
            modelMetadata.getDimension(),
            modelMetadata.getVectorDataType(),
            modelId,
            modelMetadata.getWorkloadModeConfig().toString(),
            modelMetadata.getCompressionConfig().toString(),
            ModelUtil.getMethodContextForModel(modelMetadata)
        );
        // TODO: Resolve this issue with the version
        ResolvedRequiredParameters resolvedRequiredParameters = new ResolvedRequiredParameters(
            userProvidedParameters,
            null,
            Version.V_2_14_0
        );
        return resolvedRequiredParameters.resolveKNNIndexContext(true);
    }
}
