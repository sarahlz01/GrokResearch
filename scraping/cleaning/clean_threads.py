# create the cleaned versions of the output for analysis
import re
GROK_USER_ID = "1720665183188922368"

# Leading @mentions block (remove entirely)
_LEADING_MENTIONS = re.compile(r'^(?:\s*@\w+\b[^\S\r\n]*)+')


# Leading @mentions block (capture so we can selectively keep @grok)
_LEADING_MENTIONS_BLOCK = re.compile(
    r'^(?P<block>(?:\s*[@＠][A-Za-z0-9_]{1,15}[^\S\r\n]*)+)'
)
# Plain word "grok"
_ANY_GROK_ANYWHERE = re.compile(r'(?:grok|[Ｇｇ][Ｒｒ][Ｏｏ][Ｋｋ])(?!k+(ing)|(ed))', re.IGNORECASE)
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

    s = text
    prefix = ""

    # If the text starts with a mentions block, optionally keep a single <ASSISTANT>
    # Heuristic: keep it iff (a) ONLY @grok is present, or (b) the block ends with @grok.
    m = _LEADING_MENTIONS_BLOCK.match(s)
    if m:
        block = m.group("block")
        handles = [h.lower() for h in re.findall(r'[@＠]([A-Za-z0-9_]{1,15})', block)]
        contains_grok = "grok" in handles
        if contains_grok and (len(set(handles)) == 1 or (handles and handles[-1] == "grok")):
            prefix = "<ASSISTANT> "
        # drop the whole mentions block (we already preserved conditionally)
        s = s[m.end():].lstrip()
    else:
        s = s.lstrip()

    # Special-case @grok / ＠grok → <ASSISTANT>
    s = re.sub(r'[@＠]grok(?![A-Za-z0-9_])', "<ASSISTANT>", s, flags=re.IGNORECASE)

    # Replace other mentions with stable <USER_n> tokens
    def _mention_sub(m):
        handle = m.group(1)  # captured without the @/＠
        key = handle.lower()
        if key == "grok":
            return "<ASSISTANT>"
        if key not in alias_map:
            alias_map[key] = f"<USER_{len(alias_map) + 1}>"
        return alias_map[key]

    # Mentions: support ASCII '@' and full-width '＠'; limit to valid handle chars and length
    s = re.sub(r'[@＠]([A-Za-z0-9_]{1,15})(?![A-Za-z0-9_])', _mention_sub, s)

    # Replace ANY "grok" substring anywhere (inside words/hashtags/etc.), including full-width variants
    s = _ANY_GROK_ANYWHERE.sub("<ASSISTANT>", s)

    # Replace links with <LINK>
    s = _URLS.sub("<LINK>", s)

    # Normalize whitespace, then prepend preserved leading <ASSISTANT> (if any)
    s = " ".join(s.split())
    out = (prefix + s).strip()

    # Deduplicate leading <ASSISTANT> tokens (e.g., prefix + body also starts with GROK)
    out = re.sub(r'^(?:<ASSISTANT>\s*){2,}', '<ASSISTANT> ', out)
    return out


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
