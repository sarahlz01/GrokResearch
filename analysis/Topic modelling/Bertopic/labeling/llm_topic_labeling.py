import time
import pandas as pd
import google.generativeai as genai


# API configuration
GEMINI_API_KEY = ""

genai.configure(api_key=GEMINI_API_KEY)



# Inspect available models (for  sanity check)
for model in genai.list_models():
    if "generateContent" in getattr(model, "supported_generation_methods", []):
        print(model.name, model.supported_generation_methods)



# File paths
IN_CSV = "topic_annotations_k5.csv"
OUT_CSV = "topic_annotations_k5_named.csv"



# Model selection and rate limiting
MODEL_NAME = "models/gemini-flash-latest"
SLEEP_SECONDS = 0.2  # conservative delay to reduce risk of rate limiting



# Prompt size guards
MAX_LABEL_CHARS = 200
MAX_KEYWORDS_CHARS = 400
MAX_EXAMPLE_CHARS = 1200



# Utility functions
def truncate(text, max_chars):
    """
    Truncates text to a maximum number of characters while
    preserving readability.
    """
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = str(text).replace("\n", " ").strip()
    return text if len(text) <= max_chars else text[:max_chars] + "..."


def build_prompt(topic_id, label_k5, keywords, example_text):
    """
    Constructs a controlled prompt for topic naming.
    The constraints are intentionally strict to produce
    short, publication-friendly labels.
    """
    return f"""
You are labeling topics produced by BERTopic from conversation data.

Task:
Generate a concise, human-readable topic name (2–6 words) that accurately
describes the conversation content.

Rules:
- Do NOT include the topic number.
- Do NOT use underscores.
- Use standard English capitalization.
- Be specific rather than generic.
- If the topic is ambiguous, make a reasonable best guess.

Topic ID:
{topic_id}

Existing keyword-based label:
{label_k5}

Top keywords:
{keywords}

Example conversation text:
{example_text}

Output ONLY the topic name, with no additional text.
""".strip()



# Main execution
def main():
    """
    Loads topic annotations, generates LLM-based topic names,
    and writes the enriched results back to disk.
    """
    model = genai.GenerativeModel(MODEL_NAME)

    df = pd.read_csv(IN_CSV, encoding="utf-8-sig")

    required_columns = {
        "topic",
        "label_k5",
        "keywords_top10",
        "example_doc"
    }

    if not required_columns.issubset(df.columns):
        raise RuntimeError(
            f"Input CSV must contain columns: {required_columns}"
        )

    #  output columns
    for col in ["llm_topic_name", "llm_status", "llm_error"]:
        if col not in df.columns:
            df[col] = ""

    for i, row in df.iterrows():
        # Skip rows that already have an assigned LLM label
        if str(row["llm_topic_name"]).strip():
            continue

        prompt = build_prompt(
            topic_id=row["topic"],
            label_k5=truncate(row["label_k5"], MAX_LABEL_CHARS),
            keywords=truncate(row["keywords_top10"], MAX_KEYWORDS_CHARS),
            example_text=truncate(row["example_doc"], MAX_EXAMPLE_CHARS),
        )

        try:
            response = model.generate_content(prompt)
            topic_name = response.text.strip()

            # Basic normalization
            topic_name = topic_name.replace("_", " ")
            topic_name = " ".join(topic_name.split())

            if not topic_name:
                topic_name = "Unclear topic"

            df.at[i, "llm_topic_name"] = topic_name
            df.at[i, "llm_status"] = "ok"
            df.at[i, "llm_error"] = ""

        except Exception as e:
            df.at[i, "llm_topic_name"] = ""
            df.at[i, "llm_status"] = "error"
            df.at[i, "llm_error"] = str(e)[:500]
            print(f"Topic {row['topic']} failed: {e}")

        # Periodic checkpointing to avoid data loss
        if (i + 1) % 25 == 0:
            df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
            print(f"Checkpoint saved ({i + 1}/{len(df)})")

        time.sleep(SLEEP_SECONDS)

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Labeling completed. Results saved to {OUT_CSV}")


if __name__ == "__main__":
    main()
