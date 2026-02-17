
---
license: cc-by-nc-4.0
language:
- en
- es
- ja
- pt
- fr
- tr
- ar
- hi
- id
pretty_name: '@GROKSET'
size_categories:
- 1M<n<10M
tags:
- llm
- social-media
- human-llm-interaction
- multi-party
- twitter
- x
- grok
- safety-alignment
- network-analysis
task_categories:
- text-classification
- conversation-analysis
- hate-speech-detection
---

# @GrokSet: Multi-Party Human-LLM Interactions in Social Media

<div align="center">

[![arXiv](https://img.shields.io/badge/arXiv-2503.18674-b31b1b.svg?style=for-the-badge&logoColor=white)](https://arxiv.org/abs/2503.18674)
[![Project Page](https://img.shields.io/badge/Project-Page-orange?style=for-the-badge&logo=academia&logoColor=white)]([https://www.pinlab.org/hmu](https://mamiglia.github.io/grokset/))
[![Github](https://img.shields.io/badge/Github-black?style=for-the-badge&logo=github&logoColor=white)]([https://www.pinlab.org/hmu](https://mamiglia.github.io/grokset/))


**[Matteo Migliarini](https://mamiglia.github.io/)\* · [Berat Ercevik]()\* · [Oluwagbemike Olowe]() · [Saira Fatima]() · [Sarah Zhao]() · [Minh Anh Le]() · [Vasu Sharma]() · [Ashwinee Panda]()**

*Equal contribution

</div>

## The Dataset

**@GrokSet** is the first large-scale dataset of multi-party human–LLM interactions collected from public social media. Unlike existing corpora (e.g., WildChat, LMSYS-Chat-1M) that capture private, dyadic (one-on-one) user-assistant interactions, @GrokSet captures the **Grok** Large Language Model acting as a public participant in multi-user threads on X (formerly Twitter).

The dataset spans from **March to October 2025**, covering over **1 million tweets** across **182,000+ conversation threads**. It is designed to study the behavior of LLMs in adversarial, socially embedded, and "public square" environments.

This dataset is released in a **dehydrated format** (Tweet IDs + annotations + structural metadata) to comply with platform ToS. A specialized [rehydration toolkit](https://github.com/sarahlz01/GrokResearch) is provided to reconstruct the text and metadata.

**Key Features:**
* **Multi-Party Dynamics:** Captures complex interaction graphs, not just linear queries.
* **Real-World Context:** Includes engagement metrics (likes, reposts, replies) to measure social validation.
* **Rich Annotations:** Includes pre-computed labels for **Toxicity** (Detoxify), **Topics** (BERTopic), **Trolling** (LLM-as-a-Judge), and **Network Metrics** (Centrality, Transitivity).

## Dataset Structure

The dataset is structured hierarchically around **Conversation Threads**.

**Total Statistics:**
* **Threads:** 182,707
* **Total Tweets:** 1,098,394
* **Avg. Turns per Thread:** ~6.01

### Schema Overview
The JSON structure organizes tweets chronologically within their parent thread.

```json
{
  "conversationId": "string (Unique root ID)",
  "annotations": {
    "topic": "string (e.g., '413: Parachutes in Balloon Accidents')",
    
    "network_metrics": {
      "avg_degree_centrality": "float",
      "avg_out_degree": "float",
      "reciprocity": "float",
      "transitivity": "float",
      "grok_degree_centrality": "float (Centrality of the LLM in the graph)"
    },

    "trolling": {
      "is_trolling": "string ('yes'|'no')",
      "trolling_confidence": "int (1-5)",
      "trolling_intensity": "int (1-5)",
      "trolling_category_type": "string (e.g., 'baiting', 'misinformation')",
      "troll_recognition_type": "string (e.g., 'implicitly_recognized')",
      "response_strategy_type": "string (e.g., 'serious_answer', 'corrective')",
      "assistant_tone_type": "string (e.g., 'formal', 'neutral')",
      "endorsement_type": "string (e.g., 'challenged', 'neutral')",
      "amplification_type": "string (e.g., 'no')"
    } | null,

    "discussion": {
      "is_discussion": "string ('yes'|'no')",
      "discussion_type": "string (e.g., 'political', 'scientific')",
      "bias_language": "string ('yes'|'no')",
      "assistant_stance": "string (e.g., 'neutral')",
      "user_response_type": "string (e.g., 'engaged')"
    } | null
  },

  "threads": [
    {
      "threadId": "string",
      "conversationId": "string",
      "hasMissingTweets": "boolean",
      "truncatedThread": "boolean",
      "validTweetCount": "int",
      "tweets": [
        {
          "id": "string",
          "inReplyToId": "string",
          "createdAt": "timestamp (e.g., 'Fri May 30 05:55:26 +0000 2025')",
          "lang": "string (e.g., 'en', 'pt', 'tr')",
          "text": "string (cleaned text content used for analysis)",
          "original_text": "string (Content rehydrated via ID)",
          
          "author": {
            "isVerified": "boolean",
            "followers": "int",
            "following": "int",
            "isAssistant": "boolean (True if Author is Grok)"
          },

          "metrics": {
            "likeCount": "int",
            "retweetCount": "int",
            "replyCount": "int",
            "quoteCount": "int",
            "viewCount": "int",
            "bookmarkCount": "int"
          },

          "entities": {
            "hashtags": "array",
            "urls": "array (Includes expanded_url)"
          },

          "toxicity": {
            "severe_toxicity": "float",
            "obscene": "float",
            "identity_attack": "float"
          } | null
        }
      ]
    }
  ]
}
```

_Note: Some fields (like original_text) are only available after running the rehydration script._

## Dataset Creation

### Curation Rationale

As LLMs move from private chatbots to public social agents, we lack data on how they perform in the "wild." @GrokSet was created to fill this gap, offering the first look at an LLM responding to breaking news, political polarization, and multi-user trolling in real-time.

### Data Collection

- **Period:** March 2025 – October 2025.
- **Method:** Conversations were collected using `twitterapi.io`, seeded by replies from the official Grok account. Threads were reconstructed upwards to capture the full context (including the human prompt).
- **Sampling:** Data was collected in 6-hour "block windows" to ensure temporal diversity.

### Annotation Process

The dataset includes extensive machine-generated annotations:

1. **Thematic Analysis:** 1,112 topics identified using **BERTopic** (multilingual).
2. **Safety/Toxicity:** All tweets were scored using **Detoxify** (multilingual models) to detect obscenity, threats, and hate speech.
3. **Conversational Dynamics:** All threads were annotated using **Gemini 2.0 Flash** (LLM-as-a-judge) to detect:
    - _Discussions:_ Valid back-and-forth argumentation.
    - _Trolling:_ Adversarial user behavior (baiting, sealioning).
    - _Assistant Tone:_ Whether the model mirrored user hostility.

## Bias, Risks, and Limitations

- **Survivorship Bias:** The dataset only contains tweets that were available at the time of collection. Extremely toxic content removed by X's team prior to collection is missing.
- **Western-Centric:** While multilingual, the dataset skews heavily towards English and Western political contexts.
- **Platform Specifics:** The interactions are shaped by X's specific affordances (character limits, "blue check" verification culture) and may not generalize to other platforms.

## Citation

If you use @GROKSET in your research, please cite the following paper:

```
@article{migliarini2026grokset,
  title={@GROKSET: Multi-party Human-LLM Interactions in Social Media},
  author={Migliarini, Matteo and Ercevik, Berat and Olowe, Oluwagbemike and Fatima, Saira and Zhao, Sarah and Le, Minh Anh and Sharma, Vasu and Panda, Ashwinee},
  journal={arXiv preprint},
  year={2026}
}
```

## LICENSE
The dataset annotations and structure are licensed under **CC BY-NC 4.0** The tweet content is subject to the [terms of service](https://x.com/en/tos) of X, while the original content within is owned by the original creators. This dataset is provided in a dehydrated format to respect these rights.
