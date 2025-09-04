# db.py
from __future__ import annotations
import os
import re
from typing import Optional, Iterable, List, Tuple
import pandas as pd
import mysql.connector
from mysql.connector import Error


# -------- Connection --------
def get_conn(
    mysql_host: str,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
    mysql_port: int = 3306,
):
    """
    Create a mysql-connector connection (utf8mb4).
    Requires: pip install mysql-connector-python
    """
    conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        port=int(mysql_port),
        charset="utf8mb4",
        use_unicode=True,
        autocommit=True,
    )
    return conn


get_engine = get_conn


# -------- Schema / Tables --------
def ensure_tables_exist(conn):
    """
    Create tables if not exists using mysql-connector (no SQLAlchemy).
    """
    ddl_messages = """
    CREATE TABLE IF NOT EXISTS telegram_messages_yday (
      date_label_jkt DATE,
      topic_id INT,
      topic_title VARCHAR(255),
      message_id BIGINT,
      date_utc DATETIME,
      sender_id BIGINT,
      sender_username VARCHAR(64),
      text MEDIUMTEXT,
      reply_to_msg_id BIGINT
    );
    """

    ddl_member_count = """
    CREATE TABLE IF NOT EXISTS telegram_member_count_daily (
      date_label_jkt DATE,
      chat_id BIGINT,
      chat_title VARCHAR(255),
      members_count INT,
      taken_at_utc DATETIME
    );
    """

    ddl_report = """
    CREATE TABLE IF NOT EXISTS telegram_yday_report (
      date_label_jkt DATE,
      metric VARCHAR(32),
      topic_id INT,
      topic_title VARCHAR(255),
      message_count INT,
      sender_id BIGINT,
      sender_username VARCHAR(64),
      rank_by_messages INT,
      value BIGINT
    );
    """

    with conn.cursor() as cur:
        cur.execute(ddl_messages)
        cur.execute(ddl_member_count)
        cur.execute(ddl_report)


# -------- Helpers --------
def _extract_yyyymmdd_from_filename(path_str: str) -> Optional[str]:
    """
    Get 8-digit date from filename (YYYYMMDD) → 'YYYY-MM-DD' or None.
    """
    m = re.search(r"(\d{8})", os.path.basename(path_str))
    if not m:
        return None
    y, mo, d = m.group(1)[:4], m.group(1)[4:6], m.group(1)[6:]
    return f"{y}-{mo}-{d}"


def _chunk_rows(rows: List[Tuple], size: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def _build_insert_sql(table_name: str, columns: list[str]) -> str:
    cols = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"


def _normalize_datetimes(df: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors="coerce")
            try:
                if getattr(s.dt, "tz", None) is not None:
                    s = s.dt.tz_convert(None)
            except Exception:
                try:
                    s = s.dt.tz_localize(None)
                except Exception:
                    pass
            df[col] = s


# -------- Load Parquet → MySQL (mysql-connector) --------
def load_parquet_to_mysql(
    conn,
    parquet_path: str,
    table_name: str,
    add_date_label_if_missing: bool = False,
    date_label_col: str = "date_label_jkt",
    batch_size: int = 1000,
):
    df = pd.read_parquet(parquet_path)

    if add_date_label_if_missing and date_label_col not in df.columns:
        dt_str = _extract_yyyymmdd_from_filename(parquet_path)
        if dt_str:
            df[date_label_col] = dt_str

    if table_name == "telegram_messages_yday":
        expected_cols = [
            "date_label_jkt",
            "topic_id",
            "topic_title",
            "message_id",
            "date_utc",
            "sender_id",
            "sender_username",
            "text",
            "reply_to_msg_id",
        ]
        _normalize_datetimes(df, ["date_utc"])
    elif table_name == "telegram_member_count_daily":
        expected_cols = [
            "date_label_jkt",
            "chat_id",
            "chat_title",
            "members_count",
            "taken_at_utc",
        ]
        _normalize_datetimes(df, ["taken_at_utc"])
    elif table_name == "telegram_yday_report":
        expected_cols = [
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
    else:
        expected_cols = list(df.columns)

    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[expected_cols]

    df = df.where(pd.notnull(df), None)

    sql = _build_insert_sql(table_name, expected_cols)

    rows: List[Tuple] = list(map(tuple, df.itertuples(index=False, name=None)))

    total = 0
    with conn.cursor() as cur:
        for chunk in _chunk_rows(rows, batch_size):
            cur.executemany(sql, chunk)
            total += len(chunk)

    return total
