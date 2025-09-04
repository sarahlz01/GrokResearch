# create the cleaned versions of the output for analysis
import re
GROK_USER_ID = "1720665183188922368"

# Leading @mentions block (remove entirely)
_LEADING_MENTIONS = re.compile(r'^(?:\s*@\w+\b[^\S\r\n]*)+')
# Plain word "grok"
_ANY_GROK_WORD = re.compile(r'\bgrok\b', re.IGNORECASE)
# URLs
_URLS = re.compile(r'https?://\S+')

def author_name_from_author(author: dict, alias_map: dict) -> str:
    # Grok stays "ASSISTANT"
    if str((author or {}).get("id")) == GROK_USER_ID:
        return "<ASSISTANT>"

    uname = (author or {}).get("userName")
    if not isinstance(uname, str):
        return "<USER>"  # fallback

    key = uname.lower()
    if key not in alias_map:
        alias_map[key] = f"<USER_{len(alias_map) + 1}>"
    return alias_map[key]

def clean_text_with_map(text: str, alias_map: dict) -> str:
    if not isinstance(text, str):
        return ""

    s = _LEADING_MENTIONS.sub("", text).lstrip()

    # Special-case @grok first → [ASSISTANT]
    s = re.sub(r'@grok\b', "<ASSISTANT>", s, flags=re.IGNORECASE)

    # Replace other mentions with stable <USER_n> tokens
    def _mention_sub(m):
        handle = m.group(0)[1:]
        key = handle.lower()
        if key == "grok":
            return "<ASSISTANT>"
        if key not in alias_map:
            alias_map[key] = f"<USER_{len(alias_map) + 1}>"
        return alias_map[key]

    s = re.sub(r'@(\w+)\b', _mention_sub, s)

    # Replace plain 'grok' (non-mention) with [ASSISTANT]
    s = _ANY_GROK_WORD.sub("<ASSISTANT>", s)

    # Replace links with [LINK]
    s = _URLS.sub("<LINK>", s)

    # Normalize whitespace
    return " ".join(s.split())

def clean_tweet_minimal(t: dict, alias_map: dict) -> dict:
    return {
        "text": clean_text_with_map(t.get("text", ""), alias_map),
        "authorName": author_name_from_author(t.get("author"), alias_map),
    }
def clean_conversations_minimal(out_obj: list) -> list:
    """
    Keeps {conversationId, threads[]} but trims each tweet to {text, authorName}.
    Mentions get stable per-conversation <USER_n> aliases.
    """
    cleaned = []
    for conv in out_obj or []:
        alias_map = {}  # <-- stable mapping for this conversation
        threads = []
        for th in (conv.get("threads") or []):
            new_th = {
                "threadId": th.get("threadId"),
                **({k: v for k, v in th.items() if k not in ("threadId", "tweets")}),
                "tweets": [clean_tweet_minimal(t, alias_map) for t in (th.get("tweets") or [])],
            }
            threads.append(new_th)
        cleaned.append({
            "conversationId": conv.get("conversationId"),
            "threads": threads
        })
    return cleaned
