# setup logging
import logging
logging.getLogger(__name__)

from typing import List, Optional, Tuple

def format_time_utc(ts: str) -> str:
    ts = ts.strip()
    if "_UTC" in ts: return ts
    if " " in ts: date, hms = ts.split(" ", 1)
    else: date, hms = ts, "00:00:00"
    return f"{date}_{hms}_UTC"

def build_query(handle: str,include_self_threads: bool = False, include_quotes: bool = False, include_retweets: bool = False, since: Optional[str] = None, until: Optional[str] = None) -> str:
    parts = [f"from:{handle}", "filter:replies"]
    parts.append("filter:retweets" if include_retweets else "-filter:retweets")
    parts.append("filter:quote" if include_quotes else "-filter:quote")
    parts.append("filter:self_threads" if include_self_threads else "-filter:self_threads")
    parts.append("-filter:media")
    if since: parts.append(f"since:{format_time_utc(since)}")
    if until: parts.append(f"until:{format_time_utc(until)}")
    query = " ".join(parts)
    logging.info("Built query:\t%s", query)
    return query


def extract_items(page: dict) -> Tuple[str, List[dict]]:
    if isinstance(page.get("replies"), list):
        return "replies", page.get("replies") or []
    if isinstance(page.get("tweets"), list):
        return "tweets", page.get("tweets") or []
    return "tweets", []