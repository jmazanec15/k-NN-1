import json
from abc import abstractmethod
from typing import Any, Dict, List

import numpy as np
import requests
import time

from opensearchpy import AsyncOpenSearch, AsyncHttpConnection #OpenSearch, RequestsHttpConnection

from okpt.io.config.parsers.base import ConfigurationError
from okpt.io.config.parsers.util import parse_string_param, parse_int_param, parse_dataset, parse_bool_param, \
    parse_list_param
from okpt.io.dataset import Context
from okpt.io.utils.reader import parse_json_from_path
from okpt.test.steps import base
from okpt.test.steps.base import StepConfig


class OpenSearchStep(base.Step):
    """See base class."""

    def __init__(self, step_config: StepConfig):
        super().__init__(step_config)
        self.endpoint = parse_string_param('endpoint', step_config.config,
                                           step_config.implicit_config,
                                           'localhost')
        default_port = 9200 if self.endpoint == 'localhost' else 80
        self.port = parse_int_param('port', step_config.config,
                                    step_config.implicit_config, default_port)
        self.timeout = parse_int_param('timeout', step_config.config, {}, 60)
        self.opensearch = get_opensearch_client(str(self.endpoint),
                                                int(self.port), int(self.timeout))

class BaseQueryStep(OpenSearchStep):
    """See base class."""

    def __init__(self, step_config: StepConfig):
        super().__init__(step_config)
        self.k = parse_int_param('k', step_config.config, {}, 100)
        self.r = parse_int_param('r', step_config.config, {}, 1)
        self.index_name = parse_string_param('index_name', step_config.config,
                                             {}, None)
        self.field_name = parse_string_param('field_name', step_config.config,
                                             {}, None)
        self.calculate_recall = parse_bool_param('calculate_recall',
                                                 step_config.config, {}, False)
        dataset_format = parse_string_param('dataset_format',
                                            step_config.config, {}, 'hdf5')
        dataset_path = parse_string_param('dataset_path',
                                          step_config.config, {}, None)
        self.dataset = parse_dataset(dataset_format, dataset_path,
                                     Context.QUERY)

        input_query_count = parse_int_param('query_count',
                                            step_config.config, {},
                                            self.dataset.size())
        self.query_count = min(input_query_count, self.dataset.size())

        self.neighbors_format = parse_string_param('neighbors_format',
                                                   step_config.config, {}, 'hdf5')
        self.neighbors_path = parse_string_param('neighbors_path',
                                                 step_config.config, {}, None)

    async def _action(self):

        results = {}
        query_responses = []
        for _ in range(self.query_count):
            query = self.dataset.read(1)
            if query is None:
                break
            query_responses.append(await 
                query_index(self.opensearch, self.index_name,
                            self.get_body(query[0]) , [self.field_name]))

        results['took'] = [
            float(query_response['took']) for query_response in query_responses
        ]
        results['client_time'] = [
            float(query_response['client_time']) for query_response in query_responses
        ]
        results['memory_kb'] = 0 ## get_cache_size_in_kb(self.endpoint, self.port)

        if self.calculate_recall:
            ids = [[int(hit['_id'])
                    for hit in query_response['hits']['hits']]
                   for query_response in query_responses]
            results['recall@K'] = recall_at_r(ids, self.neighbors,
                                              self.k, self.k, self.query_count)
            self.neighbors.reset()
            results[f'recall@{str(self.r)}'] = recall_at_r(
                ids, self.neighbors, self.r, self.k, self.query_count)
            self.neighbors.reset()

        self.dataset.reset()

        return results

    def _get_measures(self) -> List[str]:
        measures = ['took', 'memory_kb', 'client_time']

        if self.calculate_recall:
            measures.extend(['recall@K', f'recall@{str(self.r)}'])

        return measures

    @abstractmethod
    def get_body(self, vec):
        pass


class QueryStep(BaseQueryStep):
    """See base class."""

    label = 'query'

    def __init__(self, step_config: StepConfig):
        super().__init__(step_config)
        self.neighbors = parse_dataset(self.neighbors_format, self.neighbors_path,
                                       Context.NEIGHBORS)
        self.implicit_config = step_config.implicit_config

    def get_body(self, vec):
        return {
            'size': self.k,
            'query': {
                'knn': {
                    self.field_name: {
                        'vector': vec,
                        'k': self.k
                    }
                }
            }
        }

def recall_at_r(results, neighbor_dataset, r, k, query_count):
    """
    Calculates the recall@R for a set of queries against a ground truth nearest
    neighbor set
    Args:
        results: 2D list containing ids of results returned by OpenSearch.
        results[i][j] i refers to query, j refers to
            result in the query
        neighbor_dataset: 2D dataset containing ids of the true nearest
        neighbors for a set of queries
        r: number of top results to check if they are in the ground truth k-NN
        set.
        k: k value for the query
        query_count: number of queries
    Returns:
        Recall at R
    """
    correct = 0.0
    total_num_of_results = 0
    for query in range(query_count):
        true_neighbors = neighbor_dataset.read(1)
        if true_neighbors is None:
            break
        true_neighbors_set = set(true_neighbors[0][:k])
        true_neighbors_set.discard(-1)
        min_r = min(r, len(true_neighbors_set))
        total_num_of_results += min_r
        for j in range(min_r):
            if results[query][j] in true_neighbors_set:
                correct += 1.0

    return correct / total_num_of_results

def get_opensearch_client(endpoint: str, port: int, timeout=60):
    """
    Get an opensearch client from an endpoint and port
    Args:
        endpoint: Endpoint OpenSearch is running on
        port: Port OpenSearch is running on
        timeout: timeout for OpenSearch client, default value 60
    Returns:
        OpenSearch client

    """
    # TODO: fix for security in the future
    return AsyncOpenSearch(
        hosts=[{
            'host': endpoint,
            'port': port
        }],
        use_ssl=False,
        verify_certs=False,
        connection_class=AsyncHttpConnection,
        timeout=500,
    )

async def query_index(opensearch: AsyncOpenSearch, index_name: str, body: dict,
                excluded_fields: list):
    start_time = round(time.time()*1000)
    queryResponse = await opensearch.search(index=index_name,
                             body=body,
                             _source_excludes=excluded_fields)
    end_time = round(time.time() * 1000)
    queryResponse['client_time'] = end_time - start_time
    return queryResponse
