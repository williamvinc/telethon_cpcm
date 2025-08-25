import asyncio
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest


async def fetch_member_count(client, chat) -> int | None:
    """
    Get member count of a chat (channel or group).
    """
    while True:
        try:
            full = await client(GetFullChannelRequest(channel=chat))
            return getattr(full.full_chat, "participants_count", None)
        except FloodWaitError as e:
            wait_s = e.seconds + 1
            print(f"[!] FloodWait {wait_s}s saat ambil member_count, waiting")
            await asyncio.sleep(wait_s)
        except Exception as e:
            print(f"[!] Gagal ambil member_count: {e}")
            return None
