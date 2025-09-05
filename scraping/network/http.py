# File to make HTTP Requests
import logging

from cleaning.clean_objects_while_scraping import extract_items
logging.getLogger(__name__)
import os
import time
import logging
from typing import Optional
import requests
import backoff
from dotenv import load_dotenv

load_dotenv()
API_BASE = "https://api.twitterapi.io"
API_KEY = os.getenv("TWITTERIO_API_KEY")
HEADERS = {"X-API-Key": API_KEY}
assert API_KEY, "Set TWITTERIO_API_KEY env var."

# global variables for tracking purposes
TOTAL_API_CALLS = 0
SUCCESSFUL_API_CALLS = 0

def is_advanced_search(path: str) -> bool:
    return path == "/twitter/tweet/advanced_search"


def needs_retry_for_advanced_search(resp: requests.Response, path: str, params: dict) -> bool:
    """Return True if advanced search returned 0 tweets."""
    if not is_advanced_search(path):
        return False
    try:
        _, items = extract_items(resp.json())
        return len(items) == 0
    except Exception:
        return False


def log_success(path: str, params: dict, conversation_id: str, attempt: int, max_retries: int):
    if "tweet_ids" in params:
        logging.info(
            f"✅ Success: {path} for conversationId: {conversation_id} "
            f"calling on id: {params['tweet_ids']} (attempt {attempt}/{max_retries})"
        )
    else:
        logging.info(f"✅ Success: {path} (attempt {attempt}/{max_retries})")


@backoff.on_exception(
    backoff.expo,
    (requests.RequestException, RuntimeError),
    max_tries=3,
    jitter=None,
)
def http_get(
    path: str,
    params: Optional[dict] = None,
    conversation_id: str = None,
    timeout: int = 30,
) -> dict:
    global TOTAL_API_CALLS, SUCCESSFUL_API_CALLS

    url = f"{API_BASE}{path}"
    TOTAL_API_CALLS += 1

    resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)

    if resp.status_code != 200:
        logging.warning(
            f"⚠️ HTTP {resp.status_code} on {path}. "
            f"Response: {resp.text[:200]}..."  # truncate for readability
        )
        raise RuntimeError(f"HTTP {resp.status_code} on {url}")

    if needs_retry_for_advanced_search(resp, path, params or {}):
        logging.warning(
            f"⚠️ Returned 0 tweets for {path} "
            f"conversation: {(params or {}).get('tweetId')}. Retrying..."
        )
        raise RuntimeError("Empty advanced_search result")

    SUCCESSFUL_API_CALLS += 1
    log_success(path, params or {}, conversation_id, 1, 3)
    return resp.json()