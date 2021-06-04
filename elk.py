"""Script that downloads a public dataset and streams it to an Elasticsearch cluster"""

import csv
from os.path import abspath, join, dirname, exists
import tqdm
import urllib3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Provide a URL if directly loading from a website
DOWNLOAD_URL = (
    # "https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD"
)

#provide a localfile name
DATFILE_NAME = './data/KDD_NID.csv'
INDEX_NAME = 'kdd-events-1'
ELASTIC_URL = 'http://localhost:9200/'

DATASET_PATH = join(dirname(abspath(__file__)), DATFILE_NAME)
CHUNK_SIZE = 16384


def download_dataset():
    """Downloads the public dataset if not locally downlaoded
    and returns the number of rows are in the .csv file.
    """
    if not exists(DATASET_PATH):
        http = urllib3.PoolManager()
        resp = http.request("GET", DOWNLOAD_URL, preload_content=False)

        if resp.status != 200:
            raise RuntimeError("Could not download dataset")

        with open(DATASET_PATH, mode="wb") as f:
            chunk = resp.read(CHUNK_SIZE)
            while chunk:
                f.write(chunk)
                chunk = resp.read(CHUNK_SIZE)

    with open(DATASET_PATH) as f:
        return sum([1 for _ in f]) - 1


def create_index(client):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index=INDEX_NAME,
        body={
            "settings": {"number_of_shards": 1},
            # "mappings": {
            #     "properties": {
            #         "name": {"type": "text"},
            #         "borough": {"type": "keyword"},
            #         "cuisine": {"type": "keyword"},
            #         "grade": {"type": "keyword"},
            #         "location": {"type": "geo_point"},
            #     }
            # },
        },
        ignore=400,
    )


def generate_actions():
    """Reads the file through csv.DictReader() and for each row
    yields a single document. This function is passed into the bulk()
    helper to create many documents in sequence.
    """
    with open(DATASET_PATH, mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            doc = {
                # "_id": row["CAMIS"],
                'first_col':row['first_col'],
                'duration':row['duration'],
                'protocol_type':row['protocol_type'],
                'service':row['service'],
                'flag':row['flag'],
                'src_bytes':row['src_bytes'],
                'dst_bytes':row['dst_bytes'],
                'land':row['land'],
                'wrong_fragment':row['wrong_fragment'],
                'urgent':row['urgent'],
                'hot':row['hot'],
                'num_failed_logins':row['num_failed_logins'],
                'logged_in':row['logged_in'],
                'num_compromised':row['num_compromised'],
                'root_shell':row['root_shell'],
                'su_attempted':row['su_attempted'],
                'num_root':row['num_root'],
                'num_file_creations':row['num_file_creations'],
                'num_shells':row['num_shells'],
                'num_access_files':row['num_access_files'],
                'num_outbound_cmds':row['num_outbound_cmds'],
                'is_host_login':row['is_host_login'],
                'is_guest_login':row['is_guest_login'],
                'count':row['count'],
                'srv_count':row['srv_count'],
                'serror_rate':row['serror_rate'],
                'srv_serror_rate':row['srv_serror_rate'],
                'rerror_rate':row['rerror_rate'],
                'srv_rerror_rate':row['srv_rerror_rate'],
                'same_srv_rate':row['same_srv_rate'],
                'diff_srv_rate':row['diff_srv_rate'],
                'srv_diff_host_rate':row['srv_diff_host_rate'],
                'dst_host_count':row['dst_host_count'],
                'dst_host_srv_count':row['dst_host_srv_count'],
                'dst_host_same_srv_rate':row['dst_host_same_srv_rate'],
                'dst_host_diff_srv_rate':row['dst_host_diff_srv_rate'],
                'dst_host_same_src_port_rate':row['dst_host_same_src_port_rate'],
                'dst_host_srv_diff_host_rate':row['dst_host_srv_diff_host_rate'],
                'dst_host_serror_rate':row['dst_host_serror_rate'],
                'dst_host_srv_serror_rate':row['dst_host_srv_serror_rate'],
                'dst_host_rerror_rate':row['dst_host_rerror_rate'],
                'dst_host_srv_rerror_rate':row['dst_host_srv_rerror_rate'],
                'attack_type':row['attack_type'],
                'attack_class':row['attack_class']

            }

            # For complex documents -- inner documents

            # lat = row["Latitude"]
            # lon = row["Longitude"]
            # if lat not in ("", "0") and lon not in ("", "0"):
            #     doc["location"] = {"lat": float(lat), "lon": float(lon)}

            yield doc


def main():
    print("Loading dataset...")
    number_of_docs = download_dataset()

    client = Elasticsearch(
        # Add your cluster configuration here!
       ELASTIC_URL
    )
    print("Creating an index...")
    create_index(client)

    print("Indexing documents...")
    progress = tqdm.tqdm(unit="docs", total=number_of_docs)
    successes = 0
    for ok, action in streaming_bulk(
        client=client, index=INDEX_NAME, actions=generate_actions(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, number_of_docs))


if __name__ == "__main__":
    main()
