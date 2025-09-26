#!/usr/bin/env python3
"""
Grok Helpfulness Evaluator for Cleaned Data

This script evaluates the helpfulness of Grok responses from the cleaned JSON data format.
It analyzes responses across multiple dimensions with specific scoring standards.
"""

import pandas as pd
import json
import time
import os
from typing import List, Dict, Any
import openai
from datetime import datetime
import argparse

# Try to load from .env file if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, continue without it


class GrokHelpfulnessEvaluatorCleaned:
    def __init__(self, api_key: str, model: str = "gpt-4o"):
        """
        Initialize the evaluator with OpenAI API key and model.

        Args:
            api_key: OpenAI API key
            model: Model to use for evaluation (default: gpt-4o)
        """
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model

    def _extract_json_from_response(self, response_text: str) -> str:
        """
        Extract JSON content from a response that might be wrapped in markdown or have extra text.

        Args:
            response_text: Raw response text from the API

        Returns:
            Cleaned JSON string
        """
        text = response_text.strip()

        # Try to extract JSON from markdown code blocks
        if "```json" in text:
            start_idx = text.find("```json") + 7
            end_idx = text.rfind("```")
            if end_idx > start_idx:
                text = text[start_idx:end_idx].strip()
        elif "```" in text:
            start_idx = text.find("```") + 3
            end_idx = text.rfind("```")
            if end_idx > start_idx:
                text = text[start_idx:end_idx].strip()

        # Find JSON object boundaries if there's extra text
        if not text.startswith("{"):
            start_idx = text.find("{")
            if start_idx != -1:
                text = text[start_idx:]

        if not text.endswith("}"):
            end_idx = text.rfind("}")
            if end_idx != -1:
                text = text[: end_idx + 1]

        return text

    def _create_error_response(
        self, error_type: str, error_message: str, raw_response: str = None
    ) -> Dict[str, Any]:
        """
        Create a standardized error response with all required fields.

        Args:
            error_type: Type of error (e.g., "JSON parsing", "Validation")
            error_message: Detailed error message
            raw_response: Raw response text for debugging

        Returns:
            Standardized error response dictionary
        """
        error_response = {
            "error": f"{error_type} failed: {error_message}",
            "accuracy": {
                "reasoning": f"Evaluation failed - {error_type.lower()}",
                "score": 0,
            },
            "relevance": {
                "reasoning": f"Evaluation failed - {error_type.lower()}",
                "score": 0,
            },
            "clarity": {
                "reasoning": f"Evaluation failed - {error_type.lower()}",
                "score": 0,
            },
            "completeness": {
                "reasoning": f"Evaluation failed - {error_type.lower()}",
                "score": 0,
            },
            "usefulness": {
                "reasoning": f"Evaluation failed - {error_type.lower()}",
                "score": 0,
            },
            "tone": {
                "reasoning": f"Evaluation failed - {error_type.lower()}",
                "score": 0,
            },
            "summary": f"Evaluation failed due to {error_type.lower()} error",
        }

        if raw_response:
            error_response["raw_response"] = raw_response[:1000]  # Truncate for storage

        return error_response

    def load_conversations(self, data_path: str) -> List[Dict[str, Any]]:
        """
        Load conversations from the cleaned JSON format.

        Args:
            data_path: Path to the cleaned JSON file

        Returns:
            List of conversation dictionaries
        """
        print(f"Loading conversations from {data_path}...")

        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        print(f"Found {len(data)} conversations")
        return data

    def extract_grok_responses_from_conversation(
        self, conversation: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract Grok responses from a single conversation.

        Args:
            conversation: Single conversation dictionary

        Returns:
            List of Grok response dictionaries with context
        """
        grok_responses = []
        conversation_id = conversation.get("conversationId")

        for thread in conversation.get("threads", []):
            tweets = thread.get("tweets", [])

            # Build conversation context
            conversation_context = []
            for tweet in tweets:
                author = tweet.get("authorName", "UNKNOWN")
                text = tweet.get("text", "").strip()
                if text:
                    conversation_context.append(f"{author}: {text}")

            # Find Grok responses (ASSISTANT)
            for i, tweet in enumerate(tweets):
                if tweet.get("authorName") == "ASSISTANT":
                    # Get context leading up to this response
                    context_before = conversation_context[:i]
                    response_text = tweet.get("text", "").strip()

                    if response_text:
                        grok_responses.append(
                            {
                                "conversation_id": conversation_id,
                                "thread_id": thread.get("threadId"),
                                "response_text": response_text,
                                "context": "\n".join(context_before),
                                "full_conversation": "\n".join(conversation_context),
                            }
                        )

        return grok_responses

    def evaluate_helpfulness(self, response_text: str, context: str) -> Dict[str, Any]:
        """
        Evaluate the helpfulness of a Grok response using specific scoring standards.

        Args:
            response_text: The Grok response text
            context: The conversation context

        Returns:
            Dictionary containing evaluation scores and reasoning
        """

        evaluation_prompt = (
            f"""
You are an expert evaluator of AI ASSISTANT responses. Please evaluate the helpfulness of the following ASSISTANT response in the context of the tweet/thread conversation.

Context:
{context}

ASSISTANT Response:
{response_text}

Evaluate on the six dimensions below using a clear per-score (0–10) rubric. Assign a single integer 0–10 for each dimension based on the rubric.

Scoring rubric (apply independently to each dimension):
0: Completely not helpful for this dimension; ignores or contradicts the tweet/thread context.
1: Mentions something about the tweet but misses most key points from user tweets.
2: Answer is partly correct but does not follow the thread/context.
3: Addresses the dimension in a minimal way with major gaps or errors.
4: Partially addresses the dimension but important issues remain.
5: Adequate but basic; noticeable issues with precision, focus, or depth.
6: Generally solid with minor gaps; could be more precise or contextualized.
7: Good and reliable; small issues that do not materially affect quality.
8: Very strong; well-grounded, contextual, and helpful with only trivial issues.
9: Excellent; precise, context-aware, and highly effective for this dimension.
10: Outstanding; exemplary quality for this dimension with no observable issues.

Dimensions:
- Accuracy: Factual correctness of claims.
- Relevance: Fit to the user tweets and conversation context.
- Clarity: Organization and ease of understanding.
- Completeness: Coverage of necessary points given the tweets/context.
- Usefulness: Actionability and practical value to the user.
- Tone: Professionalism and appropriateness for the context.

For each dimension, provide:
1. Reasoning first: A concise explanation supporting the score.
2. Score: Single integer 0–10 according to the rubric above.

Respond in JSON format exactly as follows:
"""
            + """
{
    "accuracy": {"reasoning": "...", "score": 0},
    "relevance": {"reasoning": "...", "score": 0},
    "clarity": {"reasoning": "...", "score": 0},
    "completeness": {"reasoning": "...", "score": 0},
    "usefulness": {"reasoning": "...", "score": 0},
    "tone": {"reasoning": "...", "score": 0},
    "summary": "Overall assessment of the response in 1-3 sentences."
}
"""
        )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an expert evaluator of AI assistant responses. Provide detailed, objective evaluations with reasoning first, then scores. Be thorough and follow the explicit 0–10 rubric strictly. CRITICAL: Your response must be valid JSON only, with no additional text before or after the JSON object.",
                        },
                        {"role": "user", "content": evaluation_prompt},
                    ],
                    max_completion_tokens=3000,
                    temperature=0.1,  # Even lower temperature for more consistent JSON output
                )

                # Parse the JSON response
                evaluation_text = response.choices[0].message.content.strip()
                evaluation_text = self._extract_json_from_response(evaluation_text)

                evaluation = json.loads(evaluation_text)

                # Validate the structure
                required_keys = [
                    "accuracy",
                    "relevance",
                    "clarity",
                    "completeness",
                    "usefulness",
                    "tone",
                    "summary",
                ]
                for key in required_keys:
                    if key not in evaluation:
                        raise ValueError(f"Missing required key: {key}")

                # Validate score ranges
                score_keys = [
                    "accuracy",
                    "relevance",
                    "clarity",
                    "completeness",
                    "usefulness",
                    "tone",
                ]
                for key in score_keys:
                    if key in evaluation and isinstance(evaluation[key], dict):
                        score = evaluation[key].get("score")
                        if (
                            score is None
                            or not isinstance(score, int)
                            or score < 0
                            or score > 10
                        ):
                            raise ValueError(f"Invalid score for {key}: {score}")

                return evaluation

            except json.JSONDecodeError as e:
                print(f"Attempt {attempt + 1}/{max_retries}: JSON parsing error: {e}")
                print(
                    f"Raw response: {evaluation_text[:500]}..."
                )  # Truncate for readability

                if attempt < max_retries - 1:
                    print("Retrying with adjusted prompt...")
                    time.sleep(2)  # Brief pause before retry
                    # Modify the prompt to be more explicit about JSON format
                    evaluation_prompt += "\n\nIMPORTANT: Respond ONLY with valid JSON. Do not include any text before or after the JSON object. Do not use markdown formatting."
                    continue
                else:
                    return self._create_error_response(
                        "JSON parsing",
                        f"failed after {max_retries} attempts: {str(e)}",
                        evaluation_text,
                    )

            except ValueError as e:
                print(f"Attempt {attempt + 1}/{max_retries}: Validation error: {e}")
                if attempt < max_retries - 1:
                    print("Retrying...")
                    time.sleep(2)
                    continue
                else:
                    return self._create_error_response(
                        "Response validation",
                        f"failed after {max_retries} attempts: {str(e)}",
                        (
                            evaluation_text
                            if "evaluation_text" in locals()
                            else "No response received"
                        ),
                    )

            except Exception as e:
                print(f"Attempt {attempt + 1}/{max_retries}: Unexpected error: {e}")
                if attempt < max_retries - 1:
                    print("Retrying...")
                    time.sleep(2)
                    continue
                else:
                    return self._create_error_response(
                        "Unexpected error", f"after {max_retries} attempts: {str(e)}"
                    )

    def evaluate_conversations(
        self, conversations: List[Dict[str, Any]], max_conversations: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Evaluate Grok responses from the first N conversations.

        Args:
            conversations: List of conversation dictionaries
            max_conversations: Maximum number of conversations to evaluate

        Returns:
            List of evaluation results
        """
        all_evaluations = []
        conversations_to_evaluate = conversations[:max_conversations]

        print(f"Evaluating first {len(conversations_to_evaluate)} conversations...")

        for conv_idx, conversation in enumerate(conversations_to_evaluate, 1):
            conversation_id = conversation.get("conversationId")
            print(
                f"\n--- Evaluating Conversation {conv_idx}/{len(conversations_to_evaluate)}: {conversation_id} ---"
            )

            # Extract Grok responses from this conversation
            grok_responses = self.extract_grok_responses_from_conversation(conversation)

            if not grok_responses:
                print(f"No Grok responses found in conversation {conversation_id}")
                continue

            print(f"Found {len(grok_responses)} Grok responses in this conversation")

            # Evaluate each Grok response
            for resp_idx, grok_response in enumerate(grok_responses, 1):
                print(f"Evaluating response {resp_idx}/{len(grok_responses)}...")

                # Evaluate the response
                evaluation = self.evaluate_helpfulness(
                    grok_response["response_text"], grok_response["context"]
                )

                # Add metadata
                evaluation["conversation_id"] = conversation_id
                evaluation["thread_id"] = grok_response["thread_id"]
                evaluation["response_text"] = grok_response["response_text"]
                evaluation["context"] = grok_response["context"]
                evaluation["full_conversation"] = grok_response["full_conversation"]
                evaluation["response_index"] = resp_idx

                all_evaluations.append(evaluation)

                # Rate limiting - pause between requests
                time.sleep(1)

        return all_evaluations

    def save_results(self, evaluations: List[Dict[str, Any]], output_file: str):
        """
        Save evaluation results to JSON file.

        Args:
            evaluations: List of evaluation results
            output_file: Output file path
        """
        results = {
            "evaluation_date": datetime.now().isoformat(),
            "total_responses_evaluated": len(evaluations),
            "evaluations": evaluations,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"Results saved to {output_file}")

    def generate_summary_report(
        self, evaluations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Generate a summary report of all evaluations.

        Args:
            evaluations: List of evaluation results

        Returns:
            Summary statistics and insights
        """
        if not evaluations:
            return {"error": "No evaluations to summarize"}

        # Calculate average scores
        dimensions = [
            "accuracy",
            "relevance",
            "clarity",
            "completeness",
            "usefulness",
            "tone",
        ]
        summary = {}

        for dimension in dimensions:
            scores = [
                eval.get(dimension, {}).get("score", 0)
                for eval in evaluations
                if "error" not in eval and dimension in eval
            ]
            if scores:
                summary[f"avg_{dimension}"] = round(sum(scores) / len(scores), 2)
                summary[f"min_{dimension}"] = min(scores)
                summary[f"max_{dimension}"] = max(scores)
                summary[f"count_{dimension}"] = len(scores)

        # Totals
        total_non_error = sum(1 for eval in evaluations if "error" not in eval)
        summary["total_responses"] = total_non_error

        # Count errors
        error_count = sum(1 for eval in evaluations if "error" in eval)
        summary["error_count"] = error_count
        summary["successful_evaluations"] = len(evaluations) - error_count

        return summary


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate Grok response helpfulness from cleaned JSON data"
    )
    parser.add_argument(
        "json_file", help="Path to the cleaned JSON file containing conversation data"
    )
    parser.add_argument(
        "--api-key", help="OpenAI API key (or set OPENAI_API_KEY environment variable)"
    )
    parser.add_argument(
        "--model", default="gpt-4o", help="OpenAI model to use (default: gpt-4o)"
    )
    parser.add_argument(
        "--max-conversations",
        type=int,
        default=5,
        help="Maximum number of conversations to evaluate (default: 5)",
    )
    parser.add_argument(
        "--output",
        default="grok_evaluations_cleaned.json",
        help="Output file for results",
    )

    args = parser.parse_args()

    # Get API key from command line arguments or environment variables
    api_key = args.api_key or os.getenv("OPENAI_API_KEY")

    if not api_key:
        print("❌ Error: OpenAI API key not found!")
        print(
            "Please provide it using --api-key or set the OPENAI_API_KEY environment variable"
        )
        print("Example: export OPENAI_API_KEY='your-api-key-here'")
        return

    print("✅ Using OpenAI API key")

    # Initialize evaluator
    evaluator = GrokHelpfulnessEvaluatorCleaned(api_key, args.model)

    # Load conversations
    conversations = evaluator.load_conversations(args.json_file)

    if not conversations:
        print("No conversations found in the JSON file.")
        return

    # Evaluate conversations
    evaluations = evaluator.evaluate_conversations(
        conversations, args.max_conversations
    )

    if not evaluations:
        print("No Grok responses found to evaluate.")
        return

    # Save results
    evaluator.save_results(evaluations, args.output)

    # Generate and print summary
    summary = evaluator.generate_summary_report(evaluations)
    print("\n" + "=" * 60)
    print("EVALUATION SUMMARY")
    print("=" * 60)
    for key, value in summary.items():
        print(f"{key}: {value}")

    # Save summary to separate file
    summary_file = args.output.replace(".json", "_summary.json")
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to {summary_file}")


if __name__ == "__main__":
    main()

