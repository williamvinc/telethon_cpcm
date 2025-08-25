from datetime import datetime, timedelta, timezone
from typing import Tuple

JKT_OFFSET = timedelta(hours=7)


def jakarta_bounds_yesterday_utc(now_utc: datetime) -> Tuple[datetime, datetime]:
    """
    get start and end of 'yesterday' in UTC, where 'yesterday' is defined in Jakarta time (WIB, UTC+7).
    """
    now_jkt = now_utc + JKT_OFFSET
    start_today_jkt_naive = datetime(now_jkt.year, now_jkt.month, now_jkt.day)
    start_today_utc = (start_today_jkt_naive - JKT_OFFSET).replace(tzinfo=timezone.utc)
    start_yday_utc = start_today_utc - timedelta(days=1)
    return start_yday_utc, start_today_utc


def yday_label_str(now_utc: datetime) -> str:
    """lablel for 'yesterday' in Jakarta time, formatted as YYYYMMDD"""
    return (now_utc + JKT_OFFSET - timedelta(days=1)).date().strftime("%Y%m%d")
