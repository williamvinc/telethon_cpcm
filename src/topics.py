import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List

from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.tl.functions.channels import GetForumTopicsRequest


async def fetch_all_topics(client, chat) -> List:
    """
    Paging through all topics in a forum chat.
    """
    topics, seen = [], set()
    offset_date, offset_id, offset_topic = None, 0, 0
    while True:
        try:
            res = await client(
                GetForumTopicsRequest(
                    channel=chat,
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=100,
                )
            )
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


async def iter_topic_messages_yesterday(
    client,
    chat,
    topic_id: int,
    start_yday_utc: datetime,
    start_today_utc: datetime,
) -> AsyncGenerator:
    """
    Iterate through messages in a topic, from yesterday to today.
    Return: AsyncGenerator of Message
    """
    count = 0
    async for msg in client.iter_messages(
        chat, reply_to=topic_id, offset_date=start_today_utc
    ):
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


async def resolve_username(client, sender_id, cache: Dict[int, str]):
    """
    get username dari sender_id.
    """
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
