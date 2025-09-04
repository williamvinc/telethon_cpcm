import os
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import FloodWaitError

from utils_time import jakarta_bounds_yesterday_utc, yday_label_str
from topics import fetch_all_topics, iter_topic_messages_yesterday, resolve_username
from members import fetch_member_count
from reports import build_yesterday_report_parquet
from db import *

# ====== CONFIG ======
load_dotenv()
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
SESSION = os.getenv("SESSION", "william_user")
TARGET_CHAT = os.getenv("TARGET_CHAT", "")

MYSQL_HOST = os.getenv("MYSQL_HOST", "test")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

OUT_DIR = Path("telegram_dump")
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ====================


async def dump_yesterday_messages_and_member():
    now_utc = datetime.now(timezone.utc)
    start_yday_utc, start_today_utc = jakarta_bounds_yesterday_utc(now_utc)
    print(
        f"[i] JKT (UTC): {start_yday_utc.isoformat()} → {start_today_utc.isoformat()}"
    )

    async with TelegramClient(SESSION, API_ID, API_HASH) as client:
        chat = await client.get_entity(TARGET_CHAT)

        print("[i] Get topics…")
        topics = await fetch_all_topics(client, chat)
        print(f"[i] Found {len(topics)} topic.")

        # ===== Dump yesterday messages from all topics =====
        all_rows = []
        sender_cache = {}

        for t in topics:
            title = getattr(t, "title", f"topic_{t.id}")
            print(f"\n[i] Topic {t.id} — {title}")
            try:
                async for msg in iter_topic_messages_yesterday(
                    client, chat, t.id, start_yday_utc, start_today_utc
                ):
                    sender_id = getattr(msg, "sender_id", None)
                    username = await resolve_username(client, sender_id, sender_cache)
                    sender_username = (
                        username
                        if username
                        else (str(sender_id) if sender_id is not None else None)
                    )

                    all_rows.append(
                        {
                            "topic_id": t.id,
                            "topic_title": title,
                            "message_id": msg.id,
                            "date_utc": (
                                msg.date.replace(tzinfo=timezone.utc).isoformat()
                                if msg.date.tzinfo is None
                                else msg.date.isoformat()
                            ),
                            "sender_id": sender_id,
                            "sender_username": sender_username,
                            "text": msg.message or "",
                            "reply_to_msg_id": getattr(msg, "reply_to_msg_id", None),
                        }
                    )
            except FloodWaitError as e:
                wait_s = e.seconds + 1
                print(f"[!] FloodWait {wait_s}s at topic {t.id}, waiting")
                await asyncio.sleep(wait_s)

        if all_rows:
            df = pd.DataFrame(all_rows)
            df.drop_duplicates(subset=["topic_id", "message_id"], inplace=True)
            df.sort_values(["topic_id", "message_id"], inplace=True)

            yday_str = yday_label_str(now_utc)
            out_path = OUT_DIR / f"yesterday_all_topics_{yday_str}.parquet"
            df.to_parquet(out_path, index=False)
            print(f"[✓] Saved: {out_path} | total rows: {len(df)}")

            # === NEW: generate ad-hoc report (3 task → 1 parquet)
            report_path = build_yesterday_report_parquet(
                df_messages=df,
                out_dir=OUT_DIR,
                now_utc=now_utc,
                all_topics=topics,  # give all topics so totals are correct
            )
            print(f"[✓] Saved daily report: {report_path}")

        members_count = await fetch_member_count(client, chat)
        df_members = pd.DataFrame(
            [
                {
                    "date_label_jkt": (now_utc + timedelta(hours=7) - timedelta(days=1))
                    .date()
                    .isoformat(),
                    "chat_id": getattr(chat, "id", None),
                    "chat_title": getattr(chat, "title", None),
                    "members_count": members_count,
                    "taken_at_utc": now_utc.isoformat(),
                }
            ]
        )

        members_path = (
            OUT_DIR / f"yesterday_member_count_{yday_label_str(now_utc)}.parquet"
        )
        df_members.to_parquet(members_path, index=False)
        print(f"[✓] Saved member count: {members_path} | members: {members_count}")


# === async loader MySQL ===
async def load_yesterday_parquets_into_mysql():
    now_utc = datetime.now(timezone.utc)
    yday_str = yday_label_str(now_utc)

    messages_parquet = OUT_DIR / f"yesterday_all_topics_{yday_str}.parquet"
    member_parquet = OUT_DIR / f"yesterday_member_count_{yday_str}.parquet"
    report_parquet = OUT_DIR / f"yesterday_report_{yday_str}.parquet"

    paths = {
        "telegram_messages_yday": (
            messages_parquet if messages_parquet.exists() else None
        ),
        "telegram_member_count_daily": (
            member_parquet if member_parquet.exists() else None
        ),
        "telegram_yday_report": report_parquet if report_parquet.exists() else None,
    }

    def _sync_load():
        results = {}
        conn = None
        try:
            conn = get_conn(
                mysql_host=MYSQL_HOST,
                mysql_user=MYSQL_USER,
                mysql_password=MYSQL_PASSWORD,
                mysql_database=MYSQL_DATABASE,
                mysql_port=MYSQL_PORT,
            )
            ensure_tables_exist(conn)

            if paths["telegram_messages_yday"]:
                inserted = load_parquet_to_mysql(
                    conn,
                    str(paths["telegram_messages_yday"]),
                    "telegram_messages_yday",
                    True,
                    "date_label_jkt",
                )
                results["telegram_messages_yday"] = inserted
                print(f"[✓] MySQL inserted {inserted} rows -> telegram_messages_yday")
            else:
                print("[i] Skip messages: parquet not found")

            if paths["telegram_member_count_daily"]:
                inserted = load_parquet_to_mysql(
                    conn,
                    str(paths["telegram_member_count_daily"]),
                    "telegram_member_count_daily",
                    False,
                    "date_label_jkt",
                )
                results["telegram_member_count_daily"] = inserted
                print(
                    f"[✓] MySQL inserted {inserted} rows -> telegram_member_count_daily"
                )
            else:
                print("[i] Skip member_count: parquet not found")

            if paths["telegram_yday_report"]:
                inserted = load_parquet_to_mysql(
                    conn,
                    str(paths["telegram_yday_report"]),
                    "telegram_yday_report",
                    False,
                    "date_label_jkt",
                )
                results["telegram_yday_report"] = inserted
                print(f"[✓] MySQL inserted {inserted} rows -> telegram_yday_report")
            else:
                print("[i] Skip report: parquet not found")

            return results
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    return await asyncio.to_thread(_sync_load)


def main():
    async def _run():
        # 1) dump yesterday messages + member count ke parquet
        await dump_yesterday_messages_and_member()
        # 2) load parquet to MySQL
        results = await load_yesterday_parquets_into_mysql()
        print(f"[✓] MySQL load results: {results}")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
