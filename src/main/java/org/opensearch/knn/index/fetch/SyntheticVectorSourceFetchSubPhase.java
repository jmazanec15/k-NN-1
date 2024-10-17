/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.fetch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.BYTES_PER_KILOBYTES;

/**
 * Fetch sub phase that injects vectors from either doc values or knnvectors into source. Disabling source can provide
 * major storage savings, but typically comes at the cost of reduced functionality. With this, user can gain those
 * savings without sacrificing other functionality.
 */
@Log4j2
public class SyntheticVectorSourceFetchSubPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        if (shouldAddVectorSourceForShard(fetchContext) == false) {
            return null;
        }
        MapperService mapperService = fetchContext.mapperService();
        List<DocValueField> fields = new ArrayList<>();
        for (MappedFieldType mappedFieldType : mapperService.fieldTypes()) {
            if (shouldAddVectorSourceForField(mappedFieldType, fetchContext)) {
                // TODO ensure that passing right format instead of null
                fields.add(
                    new DocValueField(
                        mappedFieldType.name(),
                        mappedFieldType.valueFetcher(fetchContext.getQueryShardContext(), fetchContext.searchLookup(), null)
                    )
                );
            }
        }
        return new SyntheticVectorSourceFetchSubPhaseProcessor(fields);
    }

    private boolean shouldAddVectorSourceForShard(FetchContext fetchContext) {
        IndexSettings indexSettings = fetchContext.getIndexSettings();
        if (!KNNSettings.isKNNSyntheticSourceEnabled(indexSettings)) {
            log.debug("Synthetic is disabled for index: {}", fetchContext.getIndexName());
            return false;
        }
        if (fetchContext.fetchSourceContext().fetchSource() == false) {
            return false;
        }
        return true;
    }

    private boolean shouldAddVectorSourceForField(MappedFieldType mappedFieldType, FetchContext fetchContext) {
        if (mappedFieldType instanceof KNNVectorFieldType == false) {
            return false;
        }

        String name = mappedFieldType.name();

        String[] includes = fetchContext.fetchSourceContext().includes();
        if (Arrays.asList(includes).contains(name)) {
            return true;
        }

        String[] excludes = fetchContext.fetchSourceContext().includes();
        if (Arrays.asList(excludes).contains(name)) {
            return false;
        }

        return true;
    }

    @AllArgsConstructor
    @Getter
    static class SyntheticVectorSourceFetchSubPhaseProcessor implements FetchSubPhaseProcessor {

        // List of vector fields and associated value fetchers that should be returned in the source
        private final List<DocValueField> fields;

        @Override
        public void setNextReader(LeafReaderContext readerContext) {
            for (DocValueField f : fields) {
                f.fetcher.setNextReader(readerContext);
            }
        }

        @Override
        public void process(HitContext hitContext) throws IOException {
            SearchHit hit = hitContext.hit();
            Map<String, Object> maps = hit.getSourceAsMap();
            if (maps == null) {
                // when source is disabled, return
                return;
            }
            for (DocValueField f : fields) {
                // If the vector is already in the field, then no need to override
                if (maps.containsKey(f.field)) {
                    continue;
                }
                List<Object> docValuesSource = f.fetcher.fetchValues(hitContext.sourceLookup());
                if (docValuesSource.size() > 0) {
                    maps.put(f.field, docValuesSource.get(0));
                }
            }
            BytesStreamOutput streamOutput = new BytesStreamOutput(BYTES_PER_KILOBYTES);
            XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), streamOutput);
            builder.value(maps);
            hitContext.hit().sourceRef(BytesReference.bytes(builder));
        }
    }

    @Getter
    public static class DocValueField {
        private final String field;
        private final ValueFetcher fetcher;

        DocValueField(String field, ValueFetcher fetcher) {
            this.field = field;
            this.fetcher = fetcher;
        }
    }
}
