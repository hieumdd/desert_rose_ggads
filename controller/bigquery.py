from typing import Optional

import dateparser
from google.cloud import bigquery


def transform(rows: list[dict]) -> list[dict]:
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

    return [
        {
            "start_time": transform_date(row["start_time"]),
            "caller_phone_number": row["caller_phone_number"],
            "campaign": row["campaign"],
            "ad_group": row["ad_group"],
            "search_keyword": row["search_keyword"],
            "duration_(seconds)": row["duration_(seconds)"],
            "ad_group_bid_strategy": row["ad_group_bid_strategy"],
            "campaign_tablet_bid_adj.": row["campaign_tablet_bid_adj."],
            "campaign_mobile_bid_adj.": row["campaign_mobile_bid_adj."],
            "headline": row["headline"],
            "description_line_1": row["description_line_1"],
            "description_line_2": row["description_line_2"],
            "headline_1": row["headline_1"],
            "headline_2": row["headline_2"],
            "expanded_text_ad_headline_3": row["expanded_text_ad_headline_3"],
            "description": row["description"],
            "expanded_text_ad_description_2": row["expanded_text_ad_description_2"],
            "keyword_placement": row["keyword/placement"],
            "display_url": row["display_url"],
            "ad_final_url": row["ad_final_url"],
            "ad_mobile_final_url": row["ad_mobile_final_url"],
            "keyword_final_url": row["keyword_final_url"],
            "keyword_mobile_final_url": row["keyword_mobile_final_url"],
            "call_source": row["call_source"],
            "est_add_clicks_wk_(50_bid)": row["est._add._clicks/wk_(+50%_bid)"],
            "est._add._cost/wk_(+50%_bid)": row["est._add._cost/wk_(+50%_bid)"],
            "ad_group_target_cpa": row["ad_group_target_cpa"],
            "ad_group_max_cpm": row["ad_group_max._cpm"],
            "est_first_page_bid": row["est._first_page_bid"],
            "est_first_position_bid": row["est._first_position_bid"],
            "search_keyword_match_type": row["search_keyword_match_type"],
            "search_keyword_max_cpm": row["search_keyword_max._cpm"],
            "ad_policies": row["ad_policies"],
            "ad_group_mobile_bid_adj": row["ad_group_mobile_bid_adj."],
            "ad_group_phone_numbers_level": row["ad_group_phone_numbers_level"],
            "ad_group_phone_numbers:_active": row["ad_group_phone_numbers:_active"],
            "ad_group_sitelinks_level": row["ad_group_sitelinks_level"],
            "ad_group_sitelinks:_disapproved": row["ad_group_sitelinks:_disapproved"],
            "ad_group_sitelinks:_active": row["ad_group_sitelinks:_active"],
            "ad_group_bid_strategy_type": row["ad_group_bid_strategy_type"],
            "campaign_phone_numbers_disapproved": row[
                "campaign_phone_numbers:_disapproved"
            ],
            "campaign_phone_numbers_active": row["campaign_phone_numbers:_active"],
            "campaign_sitelinks_active": row["campaign_sitelinks:_active"],
            "campaign_sitelinks_disapproved": row["campaign_sitelinks:_disapproved"],
            "campaign_bid_strategy_type": row["campaign_bid_strategy_type"],
            "campaign_bid_strategy": row["campaign_bid_strategy"],
            "final_url": row["final_url"],
            "headline_3": row["headline_3"],
            "headline_4": row["headline_4"],
            "headline_5": row["headline_5"],
            "headline_6": row["headline_6"],
            "headline_7": row["headline_7"],
            "headline_8": row["headline_8"],
            "headline_9": row["headline_9"],
            "headline_10": row["headline_10"],
            "headline_11": row["headline_11"],
            "headline_12": row["headline_12"],
            "headline_13": row["headline_13"],
            "headline_14": row["headline_14"],
            "headline_15": row["headline_15"],
            "description_1": row["description_1"],
            "description_2": row["description_2"],
            "description_3": row["description_3"],
            "description_4": row["description_4"],
            "business_name": row["business_name"],
            "path_1": row["path_1"],
            "path_2": row["path_2"],
            "currency_code": row["currency_code"],
        }
        for row in transform_none(transform_strip(rows))
    ]


def load(rows: list[dict], client: bigquery.Client, dataset: str, table: str) -> int:
    return (
        client.load_table_from_json(
            rows,
            f"{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=[
                    {"name": "start_time", "type": "TIMESTAMP"},
                    {"name": "caller_phone_number", "type": "STRING"},
                    {"name": "campaign", "type": "STRING"},
                    {"name": "ad_group", "type": "STRING"},
                    {"name": "search_keyword", "type": "STRING"},
                    {"name": "duration_(seconds)", "type": "STRING"},
                    {"name": "ad_group_bid_strategy", "type": "STRING"},
                    {"name": "campaign_tablet_bid_adj.", "type": "STRING"},
                    {"name": "campaign_mobile_bid_adj.", "type": "STRING"},
                    {"name": "headline", "type": "STRING"},
                    {"name": "description_line_1", "type": "STRING"},
                    {"name": "description_line_2", "type": "STRING"},
                    {"name": "headline_1", "type": "STRING"},
                    {"name": "headline_2", "type": "STRING"},
                    {"name": "expanded_text_ad_headline_3", "type": "STRING"},
                    {"name": "description", "type": "STRING"},
                    {"name": "expanded_text_ad_description_2", "type": "STRING"},
                    {"name": "keyword_placement", "type": "STRING"},
                    {"name": "display_url", "type": "STRING"},
                    {"name": "ad_final_url", "type": "STRING"},
                    {"name": "ad_mobile_final_url", "type": "STRING"},
                    {"name": "keyword_final_url", "type": "STRING"},
                    {"name": "keyword_mobile_final_url", "type": "STRING"},
                    {"name": "call_source", "type": "STRING"},
                    {"name": "est_add_clicks_wk_(50_bid)", "type": "STRING"},
                    {"name": "est._add._cost/wk_(+50%_bid)", "type": "STRING"},
                    {"name": "ad_group_target_cpa", "type": "STRING"},
                    {"name": "ad_group_max_cpm", "type": "STRING"},
                    {"name": "est_first_page_bid", "type": "STRING"},
                    {"name": "est_first_position_bid", "type": "STRING"},
                    {"name": "search_keyword_match_type", "type": "STRING"},
                    {"name": "search_keyword_max_cpm", "type": "STRING"},
                    {"name": "ad_policies", "type": "STRING"},
                    {"name": "ad_group_mobile_bid_adj", "type": "STRING"},
                    {"name": "ad_group_phone_numbers_level", "type": "STRING"},
                    {"name": "ad_group_phone_numbers:_active", "type": "STRING"},
                    {"name": "ad_group_sitelinks_level", "type": "STRING"},
                    {"name": "ad_group_sitelinks:_disapproved", "type": "STRING"},
                    {"name": "ad_group_sitelinks:_active", "type": "STRING"},
                    {"name": "ad_group_bid_strategy_type", "type": "STRING"},
                    {"name": "campaign_phone_numbers_disapproved", "type": "STRING"},
                    {"name": "campaign_phone_numbers_active", "type": "STRING"},
                    {"name": "campaign_sitelinks_active", "type": "STRING"},
                    {"name": "campaign_sitelinks_disapproved", "type": "STRING"},
                    {"name": "campaign_bid_strategy_type", "type": "STRING"},
                    {"name": "campaign_bid_strategy", "type": "STRING"},
                    {"name": "final_url", "type": "STRING"},
                    {"name": "headline_3", "type": "STRING"},
                    {"name": "headline_4", "type": "STRING"},
                    {"name": "headline_5", "type": "STRING"},
                    {"name": "headline_6", "type": "STRING"},
                    {"name": "headline_7", "type": "STRING"},
                    {"name": "headline_8", "type": "STRING"},
                    {"name": "headline_9", "type": "STRING"},
                    {"name": "headline_10", "type": "STRING"},
                    {"name": "headline_11", "type": "STRING"},
                    {"name": "headline_12", "type": "STRING"},
                    {"name": "headline_13", "type": "STRING"},
                    {"name": "headline_14", "type": "STRING"},
                    {"name": "headline_15", "type": "STRING"},
                    {"name": "description_1", "type": "STRING"},
                    {"name": "description_2", "type": "STRING"},
                    {"name": "description_3", "type": "STRING"},
                    {"name": "description_4", "type": "STRING"},
                    {"name": "business_name", "type": "STRING"},
                    {"name": "path_1", "type": "STRING"},
                    {"name": "path_2", "type": "STRING"},
                    {"name": "currency_code", "type": "STRING"},
                ],
            ),
        )
        .result()
        .output_rows
    )
