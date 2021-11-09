import io
import csv
from typing import Callable

from google.cloud.storage import Client, Bucket, Blob

SUCCESS_BUCKET = ""
FAILED_BUCKET = ""


def download_to_io(client: Client, blob: Blob) -> io.StringIO:
    output = io.StringIO()
    client.download_blob_to_file(blob, output)
    return output


def move_bucket(destination: str) -> Callable[[Client, Bucket, Blob], str]:
    def move(client: Client, bucket: Bucket, blob: Blob) -> str:
        bucket.copy_blob(
            blob,
            client.bucket(destination),
            blob.name,
        )
        blob.delete()
        return f"Moved {blob.name} to {destination}"

    return move


move_to_success = move_bucket(SUCCESS_BUCKET)
move_to_failed = move_bucket(FAILED_BUCKET)


def get_data(fileio: io.StringIO) -> list[dict]:
    lines = fileio.read().splitlines()
    return [
        row
        for row in csv.DictReader(
            lines[3:],
            fieldnames=[i.replace(" ", "_").lower() for i in lines[2].split(",")],
        )
    ]
