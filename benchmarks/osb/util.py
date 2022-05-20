"""
Helper class for doing stuff
"""
import os
import sys

from opensearchpy import OpenSearch

sys.path.append(os.path.abspath(os.getcwd()))

import requests


from extensions.data_set import BigANNVectorDataSet
from extensions.util import bulk_transform

def check_exists(url, port, index_name, id):
    uri = "http://{}:{}/{}/_doc/{}".format(url, port, index_name, id)
    x = requests.head(uri)

    if x.status_code != 200:
        print(id)


def find_missing_docs(url, port, bulk_size, index_name, start_id, end_id):

    status_update = 10_000

    for i in range(start_id, end_id, bulk_size):

        if i % status_update == 0:
            print("Current id: {}".format(i))

        check_exists(url, port, index_name, i)


def redo(opensearch, bulk_size, index_name, field_name, start_id):

    def action(doc_id):
        return {'index': {'_index': index_name, '_id': doc_id}}

    data_set = BigANNVectorDataSet("/home/ec2-user/data/base.1B.u8bin")
    data_set.seek(start_id)

    partition = data_set.read(bulk_size)
    body = bulk_transform(partition, field_name, action, start_id)

    opensearch.bulk(
        body=body,
        timeout='5m'
    )


def create_client(url, port):
    return OpenSearch(
        hosts=[{'host': url, 'port': port}],
        use_ssl=False
    )


def main():
    url = "search-jmazane-hnsw5-wkw4pv6grlifazg77n4lkzu23q.us-east-2.es.amazonaws.com"
    port = 80
    #find_missing_docs(url, port, 200, "target_index", 0, 1_000_000_000)

    start_id = 0
    opensearch = create_client(url, port)
    redo(opensearch, 200, "target_index", "target_field", start_id)


if __name__ == "__main__":
    main()
