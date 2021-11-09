from typing import TypedDict

from google.cloud import bigquery, storage

from controller.storage import download_to_io, move_to_success, move_to_failed, get_data
from controller.bigquery import transform, load

BQ_CLIENT = bigquery.Client()
STORAGE_CLIENT = storage.Client()

DATASET = ""
TABLE = ""


class Context:
    event_id: str
    event_type: str


class Event(TypedDict):
    bucket: str
    name: str


class Response(TypedDict, total=False):
    status: str
    output_rows: int
    file_change: str


def run(bucket: str, filename: str) -> Response:
    source_bucket = STORAGE_CLIENT.bucket(bucket)
    source_blob = source_bucket.blob(filename)
    output = download_to_io(STORAGE_CLIENT, source_bucket, source_blob)
    try:
        output_rows = load(transform(get_data(output)), BQ_CLIENT, DATASET, TABLE)
        return {
            "status": "sucess",
            "output_rows": output_rows,
            "file_change": move_to_success(STORAGE_CLIENT, source_blob),
        }
    except:
        return {
            "status": "failed",
            "file_change": move_to_failed(STORAGE_CLIENT, source_blob),
        }


def main(event: Event, context: Context) -> Response:
    print(event)
    response = run(event["bucket"], event["name"])
    print(response)
    return response
