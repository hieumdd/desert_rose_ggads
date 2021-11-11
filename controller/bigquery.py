from datetime import datetime
from typing import Optional

import dateparser
from google.cloud import bigquery


def transform(rows: list[dict], now: datetime) -> list[dict]:
    def transform_strip(rows: list[dict]) -> list[dict]:
        return [{key: value.strip() for key, value in row.items()} for row in rows]

    def transform_none(rows: list[dict]) -> list[dict]:
        return [
            {
                **{
                    key: None
                    for key, value in row.items()
                    if value == "--" or value == ""
                },
                **{
                    key: value
                    for key, value in row.items()
                    if value != "--" and value != ""
                },
            }
            for row in rows
        ]

    def transform_date(x: str) -> Optional[str]:
        parsed = dateparser.parse(x)
        return parsed.isoformat(timespec="seconds") if parsed else None

    def safe_int(x: str) -> Optional[int]:
        return int(x) if x else None

    return [
        {
            # "start_time": transform_date(row["start_time"]),
            "caller_phone_number": row["caller_phone_number"],
            "campaign": row["campaign"],
            "campaign_id": safe_int(row["campaign_id"]),
            "ad_group": row["ad_group"],
            "ad_group_id": safe_int(row["ad_group_id"]),
            "ad_id": safe_int(row["ad_id"]),
            "search_keyword": row["search_keyword"],
            "keyword_id": safe_int(row["keyword_id"]),
            "day": row["day"],
            "_batched_at": now.isoformat(timespec='seconds')
        }
        for row in transform_none(transform_strip(rows))
    ]


def load(rows: list[dict], client: bigquery.Client, dataset: str, table: str) -> int:
    output_rows = (
        client.load_table_from_json(
            rows,
            f"{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=[
                    # {"name": "start_time", "type": "TIMESTAMP"},
                    {"name": "caller_phone_number", "type": "STRING"},
                    {"name": "campaign", "type": "STRING"},
                    {"name": "campaign_id", "type": "INTEGER"},
                    {"name": "ad_group", "type": "STRING"},
                    {"name": "ad_group_id", "type": "INTEGER"},
                    {"name": "ad_id", "type": "INTEGER"},
                    {"name": "search_keyword", "type": "STRING"},
                    {"name": "keyword_id", "type": "INTEGER"},
                    {"name": "day", "type": "DATE"},
                    {"name": "_batched_at", "type": "TIMESTAMP"},
                ],
            ),
        )
        .result()
        .output_rows
    )
    # update(client, dataset, table)
    return output_rows


def update(client: bigquery.Client, dataset: str, table: str) -> None:
    query = f"""
    CREATE OR REPLACE TABLE {dataset}.{table} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
            ROW_NUMBER() OVER
            (
                PARTITION BY caller_phone_number, campaign, campaign_id, ad_group, ad_group_id, ad_id, search_keyword, keyword_id, day
                ORDER BY _batched_at DESC
            ) AS row_num
        FROM {dataset}.{table}
    ) WHERE row_num = 1
    """
    client.query(query).result()
