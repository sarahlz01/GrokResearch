import time
import pandas as pd
import google.generativeai as genai

#API Key 
GEMINI_API_KEY = ""

genai.configure(api_key=GEMINI_API_KEY)

for m in genai.list_models():
    # show only models that support generateContent
    if "generateContent" in getattr(m, "supported_generation_methods", []):
        print(m.name, m.supported_generation_methods)


IN_CSV  = "topic_annotation_new_qwen.csv"
OUT_CSV = "topic_annotations_qwen_gemini.csv"

MODEL_NAME = "models/gemini-flash-latest"
SLEEP_SECONDS = 0.2

MAX_TOPICNAME_CHARS = 200
MAX_KEYWORDS_CHARS = 400
MAX_REP_DOC_CHARS = 1200


def truncate(s, n):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).replace("\n", " ").strip()
    return s if len(s) <= n else s[:n] + "..."


def build_prompt(topic, counts, existing_name, keywords, rep_doc):
    return f"""
You are helping label topics from topic modeling of conversation data.

Task:
Return a short, research-paper-friendly human-readable topic name (2–6 words)
that actually describes what the representative doc and keywords are about.

Rules:
- Do NOT include the topic number.
- Do NOT use underscores.
- Use normal English capitalization.
- Be more specific, not generic.
- If unclear, make the best reasonable guess.

Topic ID: {topic}
Conversation count: {counts}

Existing topic name:
{existing_name}

Top keywords:
{keywords}

Representative conversation text:
{rep_doc}

Output ONLY the topic name, nothing else.
""".strip()


def clean_llm_name(name: str) -> str:
    name = (name or "").strip()
    name = name.replace("_", " ")
    name = " ".join(name.split())
    return name if name else "Unclear topic"


def main():
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(MODEL_NAME)

    df = pd.read_csv(IN_CSV, encoding="utf-8-sig")

    required_cols = ["topic", "Counts", "Topic_name", "keywords_top10", "Representative_Docs"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV is missing columns: {missing}. Expected: {required_cols}")

    # If OUT_CSV exists, use it to resume (only the 5 columns)
    try:
        existing = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
        if all(c in existing.columns for c in required_cols) and len(existing) == len(df):
            df = existing[required_cols].copy()
            print(f"✅ Resuming from {OUT_CSV}")
    except FileNotFoundError:
        pass

    for i, row in df.iterrows():
        # skips rows already labeled (Topic_name not empty AND not just the old numeric label)
        if str(row["Topic_name"]).strip() and not str(row["Topic_name"]).strip().startswith(f"{row['topic']}:"):
            continue

        topic = row["topic"]
        counts = row["Counts"]

        existing_name = truncate(row["Topic_name"], MAX_TOPICNAME_CHARS)
        keywords = truncate(row["keywords_top10"], MAX_KEYWORDS_CHARS)
        rep_doc = truncate(row["Representative_Docs"], MAX_REP_DOC_CHARS)

        prompt = build_prompt(topic, counts, existing_name, keywords, rep_doc)

        try:
            resp = model.generate_content(prompt)
            new_name = clean_llm_name(getattr(resp, "text", ""))

            # Replace Topic_name with LLM name 
            df.at[i, "Topic_name"] = new_name

        except Exception as e:
            print(f" Topic {topic} failed: {e}")
            # leave Topic_name as-is for now

        if (i + 1) % 25 == 0:
            df[required_cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
            print(f"File checkpoint saved ({i+1}/{len(df)})")

        time.sleep(SLEEP_SECONDS)

    df[required_cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f" DONE. Saved to {OUT_CSV}")


if __name__ == "__main__":
    main()

