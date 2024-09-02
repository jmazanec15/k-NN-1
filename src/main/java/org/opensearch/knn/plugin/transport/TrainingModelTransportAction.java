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

package org.opensearch.knn.plugin.transport;

import org.opensearch.core.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.knn.index.engine.KNNLibraryIndex;
import org.opensearch.knn.index.engine.KNNLibraryIndexConfig;
import org.opensearch.knn.index.engine.KNNLibraryIndexResolver;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.memory.NativeMemoryCacheManager;
import org.opensearch.knn.index.memory.NativeMemoryEntryContext;
import org.opensearch.knn.index.memory.NativeMemoryLoadStrategy;
import org.opensearch.knn.indices.ModelMetadata;
import org.opensearch.knn.indices.ModelState;
import org.opensearch.knn.plugin.stats.KNNCounter;
import org.opensearch.knn.training.TrainingJob;
import org.opensearch.knn.training.TrainingJobRunner;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutionException;

/**
 * Transport action that trains a model and serializes it to model system index
 */
public class TrainingModelTransportAction extends HandledTransportAction<TrainingModelRequest, TrainingModelResponse> {

    private final ClusterService clusterService;

    @Inject
    public TrainingModelTransportAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(TrainingModelAction.NAME, transportService, actionFilters, TrainingModelRequest::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, TrainingModelRequest request, ActionListener<TrainingModelResponse> listener) {

        NativeMemoryEntryContext.TrainingDataEntryContext trainingDataEntryContext = new NativeMemoryEntryContext.TrainingDataEntryContext(
            request.getTrainingDataSizeInKB(),
            request.getTrainingIndex(),
            request.getTrainingField(),
            NativeMemoryLoadStrategy.TrainingLoadStrategy.getInstance(),
            clusterService,
            request.getMaximumVectorCount(),
            request.getSearchSize(),
            request.getVectorDataType()
        );

        // Allocation representing size model will occupy in memory during training
        KNNLibraryIndexConfig knnLibraryIndexConfig = request.getKnnLibraryIndexConfig();
        KNNLibraryIndex knnLibraryIndex = KNNLibraryIndexResolver.resolve(knnLibraryIndexConfig);

        NativeMemoryEntryContext.AnonymousEntryContext modelAnonymousEntryContext = new NativeMemoryEntryContext.AnonymousEntryContext(
            knnLibraryIndex.getEstimatedIndexOverhead(),
            NativeMemoryLoadStrategy.AnonymousLoadStrategy.getInstance()
        );

        TrainingJob trainingJob = new TrainingJob(
            request.getModelId(),
            NativeMemoryCacheManager.getInstance(),
            trainingDataEntryContext,
            modelAnonymousEntryContext,
            new ModelMetadata(
                knnLibraryIndexConfig.getKnnEngine(),
                knnLibraryIndexConfig.getSpaceType(),
                knnLibraryIndexConfig.getDimension(),
                ModelState.TRAINING,
                ZonedDateTime.now(ZoneOffset.UTC).toString(),
                request.getDescription(),
                "",
                clusterService.localNode().getEphemeralId(),
                knnLibraryIndexConfig.getMethodComponentContext().orElse(MethodComponentContext.EMPTY),
                knnLibraryIndexConfig.getVectorDataType(),
                knnLibraryIndexConfig.getMode(),
                knnLibraryIndexConfig.getCompressionConfig()
            )

        );

        KNNCounter.TRAINING_REQUESTS.increment();
        ActionListener<TrainingModelResponse> wrappedListener = ActionListener.wrap(listener::onResponse, ex -> {
            KNNCounter.TRAINING_ERRORS.increment();
            listener.onFailure(ex);
        });

        try {
            TrainingJobRunner.getInstance()
                .execute(
                    trainingJob,
                    ActionListener.wrap(
                        indexResponse -> wrappedListener.onResponse(new TrainingModelResponse(indexResponse.getId())),
                        wrappedListener::onFailure
                    )
                );
        } catch (IOException | ExecutionException | InterruptedException e) {
            wrappedListener.onFailure(e);
        }
    }
}
