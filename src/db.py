# db.py
from __future__ import annotations
import os
import re
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text


def get_engine(mysql_host, mysql_user, mysql_password, mysql_database, mysql_port):
    """
    Buat SQLAlchemy engine untuk MySQL (driver pymysql), charset utf8mb4.
    Pastikan 'pymysql' terinstall: pip install pymysql sqlalchemy
    """
    uri = (
        f"mysql+pymysql://{mysql_user}:{mysql_password}"
        f"{mysql_host}:{mysql_port}/{mysql_database}?charset=utf8mb4"
    )
    engine = create_engine(uri, pool_pre_ping=True, pool_recycle=3600)
    return engine


def ensure_tables_exist(engine):
    """
    Buat tabel kalau belum ada, sesuai schema 'plain' (tanpa index/PK).
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

    with engine.begin() as conn:
        conn.execute(text(ddl_messages))
        conn.execute(text(ddl_member_count))
        conn.execute(text(ddl_report))


def _extract_yyyymmdd_from_filename(path_str: str) -> Optional[str]:
    """
    Ambil 8 digit tanggal dari nama file (YYYYMMDD).
    Return 'YYYY-MM-DD' atau None.
    """
    m = re.search(r"(\d{8})", os.path.basename(path_str))
    if not m:
        return None
    y, mo, d = m.group(1)[:4], m.group(1)[4:6], m.group(1)[6:]
    return f"{y}-{mo}-{d}"


def load_parquet_to_mysql(
    engine,
    parquet_path: str,
    table_name: str,
    add_date_label_if_missing: bool = False,
    date_label_col: str = "date_label_jkt",
):
    """
    Load satu Parquet ke table MySQL (append).
    - add_date_label_if_missing: kalau True dan kolom date_label_jkt belum ada,
      kita isi dari nama file (ambil YYYYMMDD -> YYYY-MM-DD).
    """
    df = pd.read_parquet(parquet_path)

    if add_date_label_if_missing and date_label_col not in df.columns:
        dt_str = _extract_yyyymmdd_from_filename(parquet_path)
        if dt_str:
            df[date_label_col] = dt_str

    # Normalisasi kolom agar cocok dengan schema (kolom yang tidak ada akan ditambahkan sebagai NaN)
    # Mapping requirement per table:
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
    elif table_name == "telegram_member_count_daily":
        expected_cols = [
            "date_label_jkt",
            "chat_id",
            "chat_title",
            "members_count",
            "taken_at_utc",
        ]
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

    # Reorder ke expected_cols (biar aman)
    df = df[expected_cols]

    # Tipe waktu: biar aman, biarkan pandas kirim sebagai datetime (SQLAlchemy akan handle)
    # Insert batch
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    return len(df)
