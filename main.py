import os
import re
import json
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.tl.functions.channels import GetForumTopicsRequest

load_dotenv()

# ====== CONFIG ======
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
SESSION = os.getenv("SESSION", "william_user")
TARGET_CHAT = os.getenv("TARGET_CHAT", "")

OUT_DIR = Path("telegram_dump")
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ==========================

def jakarta_bounds_yesterday_utc(now_utc: datetime):
    jkt_offset = timedelta(hours=7)
    now_jkt = now_utc + jkt_offset
    start_today_jkt_naive = datetime(now_jkt.year, now_jkt.month, now_jkt.day)
    start_today_utc = (start_today_jkt_naive - jkt_offset).replace(tzinfo=timezone.utc)
    start_yday_utc = start_today_utc - timedelta(days=1)
    return start_yday_utc, start_today_utc

async def fetch_all_topics(client, chat):
    topics, seen = [], set()
    offset_date, offset_id, offset_topic = None, 0, 0
    while True:
        try:
            res = await client(GetForumTopicsRequest(
                channel=chat,
                offset_date=offset_date,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=100
            ))
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
            continue

        if not res.topics:
            break

        for t in res.topics:
            if t.id not in seen:
                seen.add(t.id)
                topics.append(t)

        last = res.topics[-1]
        offset_topic, offset_id, offset_date = last.id, 0, None
        if len(res.topics) < 100:
            break
    return topics

async def iter_topic_messages_yesterday(client, chat, topic_id: int,
                                        start_yday_utc: datetime, start_today_utc: datetime):
    count = 0
    async for msg in client.iter_messages(chat, reply_to=topic_id, offset_date=start_today_utc):
        msg_dt = msg.date
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=timezone.utc)

        if msg_dt < start_yday_utc:
            break
        if msg_dt >= start_today_utc:
            continue

        yield msg
        count += 1
        if count % 200 == 0:
            await asyncio.sleep(0.5)

async def resolve_username(client, sender_id, cache: dict):
    if sender_id is None:
        return None
    if sender_id in cache:
        return cache[sender_id]
    try:
        ent = await client.get_entity(sender_id)
    except Exception:
        cache[sender_id] = None
        return None

    username = getattr(ent, "username", None)
    cache[sender_id] = username
    return username

async def main():
    now_utc = datetime.now(timezone.utc)
    start_yday_utc, start_today_utc = jakarta_bounds_yesterday_utc(now_utc)
    print(f"[i] JKT (UTC): {start_yday_utc.isoformat()} → {start_today_utc.isoformat()}")

    async with TelegramClient(SESSION, API_ID, API_HASH) as client:
        chat = await client.get_entity(TARGET_CHAT)

        print("[i] Get topics…")
        topics = await fetch_all_topics(client, chat)
        print(f"[i] Found {len(topics)} topic.")

        all_rows = []
        total_msgs = 0
        sender_cache = {}

        for t in topics:
            title = getattr(t, "title", f"topic_{t.id}")
            print(f"\n[i] Topic {t.id} — {title}")

            try:
                async for msg in iter_topic_messages_yesterday(
                    client, chat, t.id, start_yday_utc, start_today_utc
                ):
                    sender_id = getattr(msg, "sender_id", None)
                    sender_username = await resolve_username(client, sender_id, sender_cache)
                    sender_username = sender_username if sender_username else (str(sender_id) if sender_id is not None else None)

                    all_rows.append({
                        "topic_id": t.id,
                        "topic_title": title,
                        "message_id": msg.id,
                        "date_utc": msg.date.replace(tzinfo=timezone.utc).isoformat()
                                    if msg.date.tzinfo is None else msg.date.isoformat(),
                        "sender_id": sender_id,
                        "sender_username": sender_username,
                        "text": msg.message or "",
                        "reply_to_msg_id": getattr(msg, "reply_to_msg_id", None),
                    })
                    total_msgs += 1

            except FloodWaitError as e:
                wait_s = e.seconds + 1
                print(f"[!] FloodWait {wait_s}s at topic {t.id}, waiting")
                await asyncio.sleep(wait_s)

        if not all_rows:
            print("\n[✓] No message from yesterday")
            return

        df = pd.DataFrame(all_rows)
        df.drop_duplicates(subset=["topic_id", "message_id"], inplace=True)
        df.sort_values(["topic_id", "message_id"], inplace=True)

        yday_jkt = (now_utc + timedelta(hours=7) - timedelta(days=1)).date().strftime("%Y%m%d")
        out_path = OUT_DIR / f"yesterday_all_topics_{yday_jkt}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"\n[✓] Saved: {out_path} | total rows: {len(df)}")

if __name__ == "__main__":
    asyncio.run(main())
