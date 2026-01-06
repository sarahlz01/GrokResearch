import json
import pandas as pd
from openai import OpenAI


client = OpenAI(api_key="") 

def load_json_data(filepath: str, max_conversations=None):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if max_conversations:
        data = data[:max_conversations]  
    print(f"Loaded {len(data)} conversations from {filepath}")
    return data


def preprocess_data(data):
    rows = []
    for conv in data:
        conv_id = conv.get("conversationId")
        combined_text = ""
        for thread in conv.get("threads", []):
            for tweet in thread.get("tweets", []):
                combined_text += tweet.get("text", "") + " "
        if combined_text.strip():
            rows.append({
                "conversationId": conv_id,
                "text": combined_text.strip()
            })
    df = pd.DataFrame(rows)
    print(f"Prepared {len(df)} conversations for GPT")
    return df


# Topic Analysis with GPT (small demo)

def topic_analysis_openai(df: pd.DataFrame, batch_size=1):
    texts = df["text"].tolist()
    topics_data = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        print(f"Processing batch {i // batch_size + 1} / {(len(texts) - 1) // batch_size + 1}...")
        batch_text = "\n\n".join([f"- {t}" for t in batch])

        prompt = f"""
        You are an expert data analyst. I will give you {len(batch)} conversation texts.
        Identify and group them into distinct topics.
        For each topic, provide:
        1. A short topic name (max 5 words)
        2. 5-10 representative keywords
        3. One representative example text from the batch

        Here are the texts:
        {batch_text}

        Return ONLY a valid JSON array of objects with keys: "topic", "keywords", "example".
        """

        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful topic modeling assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )

            gpt_output = response.choices[0].message.content
            try:
                topics_batch = json.loads(gpt_output)
                topics_data.extend(topics_batch)
            except Exception as e:
                print("Error parsing GPT output:", e)
                print("GPT raw output:", gpt_output)

        except Exception as e:
            print("Error calling OpenAI API:", e)

    if topics_data:
        topic_df = pd.DataFrame(topics_data)
        topic_df.to_csv("topic_summary_demo.csv", index=False, encoding="utf-8-sig")
        print("Demo topic analysis complete. Results saved to topic_summary_demo.csv")
        print(topic_df.head())
        return topic_df
    else:
        print("No topics were generated.")
        return pd.DataFrame()


if __name__ == "__main__":
    filepath = "grok_data/output_CLEANED.json"
    
    # I have taken only the first 5 conversations for a quick  demo
    data = load_json_data(filepath, max_conversations=5)
    
    df = preprocess_data(data)

    # GPT topic analysis with batch_size=1 for speed
    topic_df = topic_analysis_openai(df, batch_size=1)