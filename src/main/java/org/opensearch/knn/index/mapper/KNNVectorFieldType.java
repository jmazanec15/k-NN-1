/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.mapper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.knn.index.KNNVectorIndexFieldData;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNIndexContext;
import org.opensearch.knn.index.engine.KNNLibrarySearchContext;
import org.opensearch.knn.index.engine.model.QueryContext;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.opensearch.knn.index.mapper.KNNVectorFieldMapperUtil.deserializeStoredVector;

/**
 * A KNNVector field type to represent the vector field in Opensearch
 */
public class KNNVectorFieldType extends MappedFieldType {
    // For model based indices, the KNNVectorFieldTypeConfig cannot be created during mapping parsing. This is due to
    // mapping parsing happening during node recovery, when the cluster state (containing information about the model)
    // is not available. To workaround this, the field type is configured with a supplier. To ensure proper access,
    // the config is wrapped in this private class, CachedKNNVectorFieldTypeConfig
    private final CachedKNNVectorFieldTypeConfig cachedKNNVectorFieldTypeConfig;
    private final String modelId;

    /**
     * Constructor for KNNVectorFieldType.
     *
     * @param name name of the field
     * @param metadata metadata of the field
     * @param knnVectorFieldTypeConfigSupplier Supplier for {@link KNNVectorFieldTypeConfig}
     */
    public KNNVectorFieldType(
        String name,
        Map<String, String> metadata,
        Supplier<KNNVectorFieldTypeConfig> knnVectorFieldTypeConfigSupplier,
        String modelId
    ) {
        super(name, false, false, true, TextSearchInfo.NONE, metadata);
        this.cachedKNNVectorFieldTypeConfig = new CachedKNNVectorFieldTypeConfig(knnVectorFieldTypeConfigSupplier);
        this.modelId = modelId;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        throw new UnsupportedOperationException("KNN Vector do not support fields search");
    }

    @Override
    public String typeName() {
        return KNNVectorFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        return new DocValuesFieldExistsQuery(name());
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new QueryShardException(
            context,
            String.format(Locale.ROOT, "KNN vector do not support exact searching, use KNN queries instead: [%s]", name())
        );
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        failIfNoDocValues();
        return new KNNVectorIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES, getVectorDataType());
    }

    @Override
    public Object valueForDisplay(Object value) {
        return deserializeStoredVector((BytesRef) value, getVectorDataType());
    }

    public Map<String, Object> getLibraryParameters() {
        return cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getKnnIndexContext().getLibraryParameters();
    }

    public KNNEngine getKNNEngine() {
        return cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getKnnEngine();
    }

    /**
     * Get the dimension for the field
     *
     * @return the vector dimension of the field.
     */
    public int getDimension() {
        return cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getDimension();
    }

    /**
     * Get the vector data type of the field
     *
     * @return the vector data type of the field
     */
    public VectorDataType getVectorDataType() {
        return cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getVectorDataType();
    }

    /**
     * Get the model id if the field is configured to have it. Null otherwise.
     *
     * @return the model id if the field is built for ann-indexing, empty otherwise
     */
    public Optional<String> getModelId() {
        return Optional.ofNullable(modelId);
    }

    /**
     * Determine whether the field is built for ann-indexing. If not, only brute force search is available
     *
     * @return true if the field is built for ann-indexing, false otherwise
     */
    public boolean isIndexedForAnn() {
        return getModelId().isPresent() || getKNNIndexContext().isPresent();
    }

    /**
     * Return a map of query parameters that are valid for the given query context and augmented with other
     * parameters
     *
     * @param queryContext Context of the query
     * @param originalMethodParameters user provided query parameters
     * @return parameters to be passed to the library augmented based on the field type
     */
    public Map<String, Object> getProcessedQueryMethodParameters(QueryContext queryContext, Map<String, Object> originalMethodParameters) {
        if (originalMethodParameters == null || originalMethodParameters.isEmpty()) {
            return originalMethodParameters;
        }

        // If we are unable to get the configuration and the user is trying to passs in parameters, we have to fail
        // the request
        KNNIndexContext knnIndexContext = getKNNIndexContext().orElseThrow(
            () -> new IllegalArgumentException(
                "Unable to validate passed in method parameters because index was built with model before 2.14"
            )
        );

        final KNNLibrarySearchContext engineSpecificMethodContext = knnIndexContext.getKnnLibrarySearchContext();
        return engineSpecificMethodContext.processMethodParameters(queryContext, originalMethodParameters);
    }

    public RescoreContext getProcessedRescoreQueryContext(QueryContext queryContext, RescoreContext originalRescoreContext) {
        if (originalRescoreContext != null) {
            return originalRescoreContext;
        }
        Optional<KNNIndexContext> knnIndexContext = getKNNIndexContext();
        return knnIndexContext.map(indexContext -> indexContext.getKnnLibrarySearchContext().getDefaultRescoreContext(queryContext))
            .orElse(RescoreContext.DISABLED_RESCORE_CONTEXT);
    }

    Optional<KNNIndexContext> getKNNIndexContext() {
        KNNVectorFieldTypeConfig knnVectorFieldTypeConfig = cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig();
        if (knnVectorFieldTypeConfig == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(knnVectorFieldTypeConfig.getKnnIndexContext());
    }

    public SpaceType getSpaceType() {
        return cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getSpaceType();
    }

    /**
     * Configuration class for {@link KNNVectorFieldType}
     */
    @AllArgsConstructor
    @Builder
    @Getter
    public static final class KNNVectorFieldTypeConfig {
        private final int dimension;
        private final VectorDataType vectorDataType;
        private final KNNIndexContext knnIndexContext;
        private final SpaceType spaceType;
        private final KNNEngine knnEngine;
    }

    @RequiredArgsConstructor
    private static class CachedKNNVectorFieldTypeConfig {
        private final Supplier<KNNVectorFieldTypeConfig> knnVectorFieldTypeConfigSupplier;
        private KNNVectorFieldTypeConfig cachedKnnVectorFieldTypeConfig;

        private KNNVectorFieldTypeConfig getKnnVectorFieldTypeConfig() {
            if (cachedKnnVectorFieldTypeConfig == null) {
                initKNNVectorFieldTypeConfig();
            }
            return cachedKnnVectorFieldTypeConfig;
        }

        private synchronized void initKNNVectorFieldTypeConfig() {
            if (cachedKnnVectorFieldTypeConfig == null) {
                cachedKnnVectorFieldTypeConfig = knnVectorFieldTypeConfigSupplier.get();
            }
        }
    }
}
