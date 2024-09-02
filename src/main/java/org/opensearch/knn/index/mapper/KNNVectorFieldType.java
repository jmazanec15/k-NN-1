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
import org.opensearch.Version;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.knn.index.KNNVectorIndexFieldData;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.DefaultKNNLibraryIndexSearchResolver;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNLibraryIndex;
import org.opensearch.knn.index.engine.KNNLibraryIndexConfig;
import org.opensearch.knn.index.engine.KNNLibraryIndexSearchResolver;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.engine.config.CompressionConfig;
import org.opensearch.knn.index.engine.config.WorkloadModeConfig;
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
        return cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getKnnLibraryIndex().getLibraryParameters();
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
        return modelId != null || getKNNLibraryIndex().isPresent();
    }

    public KNNEngine getKNNEngine() {
        KNNEngine knnEngine = cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig().getKnnEngine();
        if (knnEngine == null) {
            throw new IllegalArgumentException("Invaliid no engine");
        }
        return knnEngine;
    }

    public KNNLibraryIndexSearchResolver getKnnLibraryIndexSearchResolver() {
        if (isIndexedForAnn() == false) {
            throw new IllegalArgumentException("FIX ME");
        }

        if (getKNNLibraryIndex().isEmpty()) {
            // TODO: This case needs to be handeld more gracefully. Maybe pass in the config via field type
            return new DefaultKNNLibraryIndexSearchResolver(
                new KNNLibraryIndexConfig(
                    getVectorDataType(),
                    getSpaceType(),
                    getKNNEngine(),
                    getDimension(),
                    Version.V_EMPTY,
                    MethodComponentContext.EMPTY,
                    WorkloadModeConfig.NOT_CONFIGURED,
                    CompressionConfig.NOT_CONFIGURED,
                    true
                )
            );
        }

        return getKNNLibraryIndex().get().getKnnLibraryIndexSearchResolver();
    }

    Optional<KNNLibraryIndex> getKNNLibraryIndex() {
        KNNVectorFieldTypeConfig knnVectorFieldTypeConfig = cachedKNNVectorFieldTypeConfig.getKnnVectorFieldTypeConfig();
        if (knnVectorFieldTypeConfig == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(knnVectorFieldTypeConfig.getKnnLibraryIndex());
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
        private final SpaceType spaceType;
        private final KNNEngine knnEngine;
        // null in the case of old model and/or flat mapper
        private final KNNLibraryIndex knnLibraryIndex;
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
