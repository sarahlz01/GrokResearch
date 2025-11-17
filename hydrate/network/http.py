# File to make HTTP Requests
import logging

from cleaning.clean_objects_while_scraping import extract_items
logging.getLogger(__name__)
import os
import time
import logging
from typing import Optional
import requests
from dotenv import load_dotenv

load_dotenv()
API_BASE = "https://api.twitterapi.io"
API_KEY = os.getenv("TWITTERIO_API_KEY")
HEADERS = {"X-API-Key": API_KEY}
assert API_KEY, "Set TWITTERIO_API_KEY env var."

# global variables for tracking purposes
TOTAL_API_CALLS = 0
SUCCESSFUL_API_CALLS = 0


def log_success(path: str, params: dict, conversation_id: str, attempt: int, max_retries: int):
    p = params or {}
    if p.get("tweet_ids"): # if we are doing a /twitter/tweets call, we have conversation id and current id
        logging.info("✅\tSuccess: %s for conversationId: %s\tcalling on id: %s (attempt %d/%d)", path, conversation_id, p.get("tweet_ids"), attempt, max_retries)
    else: # if we are doing any other call (like advanced_search)
        logging.info("✅\tSuccess: %s (attempt %d/%d)", path, attempt, max_retries)
    
# Makes ONE http request
def http_get(path: str, params: Optional[dict] = None, conversation_id: str = None, max_retries: int = 3, timeout: int = 30) -> dict:
    global TOTAL_API_CALLS, SUCCESSFUL_API_CALLS

    url = f"{API_BASE}{path}"
    backoff = 5
    last_execution_error = None # track the last error

    for attempt in range(1, max_retries+1):
        try:
            TOTAL_API_CALLS += 1
            resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            
            # if the call was a advanced_search, sometimes it returns 0 tweets. if so, then recall it just in case
            if (path == "/twitter/tweet/advanced_search"):
                _, res_ = extract_items(resp.json())
                if len(res_) == 0:
                    p = params or {}
                    logging.warning("⚠️\tReturned 0 tweets for %s conversation: %s, retrying (%d/%d). Backing off %.1f s... | VERBOSE : %s", path, p.get("tweetId"), attempt, max_retries, backoff, resp.text)
                    time.sleep(5) # removed backoff
                    continue
                
            # check response status code
            if resp.status_code == 200:
                SUCCESSFUL_API_CALLS += 1
                log_success(path=path, params=params, conversation_id=conversation_id, attempt=attempt, max_retries=max_retries)
                return resp.json()
            else:
                logging.warning("⚠️\tHTTP %s on %s (%d/%d). Backing off %.1f s... | VERBOSE : %s", resp.status_code, path, attempt, max_retries, backoff, resp.text)
                time.sleep(backoff)
                backoff *= 2
        except Exception as e:
            logging.warning("🚫\tError on %s (%d/%d): %s. Backing off %.1f s...", path, attempt, max_retries, e, backoff,)
            last_execution_error = e
            time.sleep(backoff)
            backoff *= 2
            continue

    logging.error("🚨\tFailed after %d attempts on %s", max_retries, path)
    if last_execution_error:
        raise last_execution_error
    else:
        raise RuntimeError(f"Failed to fetch {url}")