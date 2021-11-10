import io
import csv
from datetime import datetime

from google.cloud.storage import Bucket, Blob


def download_to_io(blob: Blob) -> io.StringIO:
    return io.StringIO(blob.download_as_string().decode("utf-8"))


def move_bucket(destination: Bucket, bucket: Bucket, blob: Blob, now: datetime) -> str:
    filename, ext = blob.name.split(".")
    xx = f"{filename}-{now.strftime('%Y%m%d')}.{ext}"
    bucket.copy_blob(
        blob=blob,
        destination_bucket=destination,
        new_name=f"{filename}-{now.strftime('%Y%m%d')}.{ext}",
    )
    blob.delete()
    return f"Moved {blob.name} to {destination}"


def get_data(fileio: io.StringIO) -> list[dict]:
    lines = fileio.read().splitlines()
    return [
        row
        for row in csv.DictReader(
            lines[3:],
            fieldnames=[i.replace(" ", "_").lower() for i in lines[2].split(",")],
        )
    ]
