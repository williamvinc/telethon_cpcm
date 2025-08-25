from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd

JKT_OFFSET = timedelta(hours=7)


def _yday_label_str(now_utc: datetime) -> str:
    return (now_utc + JKT_OFFSET - timedelta(days=1)).date().strftime("%Y%m%d")


def _yday_label_iso(now_utc: datetime) -> str:
    return (now_utc + JKT_OFFSET - timedelta(days=1)).date().isoformat()


def build_yesterday_report_parquet(
    df_messages: pd.DataFrame,
    out_dir: Path,
    now_utc: datetime,
    all_topics: list | None = None,
) -> Path:
    """
    Generate a daily report parquet file containing:
    1) Messages per topic
    2) Top contributors
    3) Summary counts (topics_count, messages_total)
    """
    date_label_jkt_iso = _yday_label_iso(now_utc)
    yday_str = _yday_label_str(now_utc)

    # --- 1) Messages per topic
    if not df_messages.empty:
        g_topic = (
            df_messages.groupby(["topic_id", "topic_title"], dropna=False)
            .size()
            .reset_index(name="message_count")
        )
        df_topic = g_topic.assign(
            date_label_jkt=date_label_jkt_iso,
            metric="messages_per_topic",
        )[["date_label_jkt", "metric", "topic_id", "topic_title", "message_count"]]
    else:
        df_topic = pd.DataFrame(
            columns=[
                "date_label_jkt",
                "metric",
                "topic_id",
                "topic_title",
                "message_count",
            ]
        )

    # --- 2) Top contributors
    if not df_messages.empty:
        g_sender = (
            df_messages.groupby(["sender_id", "sender_username"], dropna=False)
            .size()
            .reset_index(name="message_count")
            .sort_values(["message_count", "sender_username"], ascending=[False, True])
        )
        g_sender["rank_by_messages"] = (
            g_sender["message_count"].rank(method="dense", ascending=False).astype(int)
        )

        df_contrib = g_sender.assign(
            date_label_jkt=date_label_jkt_iso,
            metric="contributors",
        )[
            [
                "date_label_jkt",
                "metric",
                "sender_id",
                "sender_username",
                "message_count",
                "rank_by_messages",
            ]
        ]
    else:
        df_contrib = pd.DataFrame(
            columns=[
                "date_label_jkt",
                "metric",
                "sender_id",
                "sender_username",
                "message_count",
                "rank_by_messages",
            ]
        )

    # --- 3) Summary counts
    topics_count = (
        int(df_messages["topic_id"].nunique()) if not df_messages.empty else 0
    )
    messages_total = int(len(df_messages)) if not df_messages.empty else 0

    topics_count = None
    if all_topics is not None:
        topics_count = len(all_topics)
    else:
        topics_count = (
            int(df_messages["topic_id"].nunique()) if not df_messages.empty else 0
        )

    messages_total = int(len(df_messages)) if not df_messages.empty else 0

    df_summary = pd.DataFrame(
        [
            {
                "date_label_jkt": date_label_jkt_iso,
                "metric": "topics_count",
                "value": topics_count,
            },
            {
                "date_label_jkt": date_label_jkt_iso,
                "metric": "messages_total",
                "value": messages_total,
            },
        ]
    )[["date_label_jkt", "metric", "value"]]

    superset_cols = [
        "date_label_jkt",
        "metric",
        "topic_id",
        "topic_title",
        "message_count",
        "sender_id",
        "sender_username",
        "rank_by_messages",
        "value",
    ]
    for df_ in (df_topic, df_contrib, df_summary):
        for col in superset_cols:
            if col not in df_.columns:
                df_[col] = pd.NA
        df_.reset_index(drop=True, inplace=True)
        df_ = df_[superset_cols]

    df_all = pd.concat(
        [df_topic[superset_cols], df_contrib[superset_cols], df_summary[superset_cols]],
        ignore_index=True,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"yesterday_report_{yday_str}.parquet"
    df_all.to_parquet(out_path, index=False)
    return out_path
