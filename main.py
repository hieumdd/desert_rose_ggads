import os
from typing import TypedDict

from google.cloud import bigquery, storage

from controller.storage import download_to_io, get_data, move_bucket
from controller.bigquery import transform, load

BQ_CLIENT = bigquery.Client()
STORAGE_CLIENT = storage.Client()

DATASET = "GoogleAdsExport"
TABLE = "CallTrackingCSV"

UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
SUCCESS_BUCKET = "desert-rose-gg-ads-csv-success"
FAILED_BUCKET = "desert-rose-gg-ads-csv-failed"


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
    output = download_to_io(STORAGE_CLIENT, source_blob)
    try:
        output_rows = load(transform(get_data(output)), BQ_CLIENT, DATASET, TABLE)
        return {
            "status": "sucess",
            "output_rows": output_rows,
            "file_change": move_bucket(
                SUCCESS_BUCKET, source_bucket, source_blob
            ),
        }
    except:
        return {
            "status": "failed",
            "file_change": move_bucket(
                FAILED_BUCKET, source_bucket, source_blob
            ),
        }


def main(event: Event, context: Context) -> Response:
    print(event, context)
    response = run(event["bucket"], event["name"])
    print(response)
    return response
