import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

from config import AnalysisConfig
from detoxify import Detoxify
from langdetect import DetectorFactory, detect

logger = logging.getLogger(__name__)


DetectorFactory.seed = 0


class ToxicityAnalyzer:
    def __init__(self, config: AnalysisConfig):
        self.config = config

        self.english_model = Detoxify(self.config.english_model_name)
        self.multilingual_model = Detoxify(self.config.multilingual_model_name)
        self.semaphore = asyncio.Semaphore(self.config.max_prediction_concurrency)
        logger.info(f"Initialized ToxicityAnalyzer with models: {self.config.english_model_name} and {self.config.multilingual_model_name}. Concurrency Limit: {self.config.max_prediction_concurrency}")

    async def analyze_single_reply(self, reply_item: dict) -> Dict:
        user_prompt = reply_item.get('user_prompt', '')
        grok_reply = reply_item.get('grok_reply', '')
        conversation_id = reply_item.get('conversationId')
        thread_id = reply_item.get('threadId', 'N/A')
        role = reply_item.get('role', '')

        if not user_prompt:
            return {}

        specific_label_thresholds = {
            # 'toxicity': 0.9,
            'threat': 0.9, 
            'severe_toxicity': 0.9, 
            'identity_attack': 0.9,
            'insult': 0.9, 
            'sexual_explicit': 0.66, 
            'obscene': 0.6,
        }
        general_toxicity_threshold = 0.90

        try:
            language = await asyncio.to_thread(detect, user_prompt)
        except Exception:
            language = "unknown"

        if not language:
            return {"conversationId": conversation_id, "threadId": thread_id, "user_prompt": user_prompt, "grok_reply": grok_reply, "user_prompt_toxicity_score": 0.0, "grok_reply_toxicity_score": 0.0, "user_prompt_category": "unknown_language", "grok_reply_category": "unknown_language", "role": role}

        async with self.semaphore:
            logger.debug(f"Starting prediction. Active predictions: {self.config.max_prediction_concurrency - self.semaphore._value} / {self.config.max_prediction_concurrency}")

            try:
                if language == "en":
                    user_prompt_prediction = await asyncio.to_thread(self.english_model.predict, user_prompt)
                    grok_reply_prediction = await asyncio.to_thread(self.english_model.predict, grok_reply)
                else:
                    user_prompt_prediction = await asyncio.to_thread(self.multilingual_model.predict, user_prompt)
                    grok_reply_prediction = await asyncio.to_thread(self.multilingual_model.predict, grok_reply)
            except Exception as e:
                logger.error(f"Prediction failed for {conversation_id}/{thread_id}: {e}")
                return {"conversationId": conversation_id, "threadId": thread_id, "user_prompt": user_prompt, "grok_reply": grok_reply, "user_prompt_toxicity_score": 0.0, "grok_reply_toxicity_score": 0.0, "user_prompt_category": "prediction_error", "grok_reply_category": "prediction_error", "role": role}

        user_prompt_toxicity_scores = {label: round(score, 4) for label, score in user_prompt_prediction.items()}
        grok_reply_toxicity_scores = {label: round(score, 4) for label, score in grok_reply_prediction.items()}

        # user prompt - pick the highest scoring label across all specific labels
        user_prompt_label_scores = {
            label: float(user_prompt_toxicity_scores.get(label, 0.0))
            for label in specific_label_thresholds
        }
        logger.debug(f"User prompt label scores for {conversation_id}/{thread_id}: {user_prompt_label_scores}")

        final_user_prompt_category = max(user_prompt_label_scores, key=user_prompt_label_scores.get)
        final_user_prompt_score = user_prompt_label_scores[final_user_prompt_category]

        # Check if the score meets the threshold for that label or the general toxicity threshold
        user_prompt_general_toxicity = float(user_prompt_toxicity_scores.get('toxicity', 0.0))
        if final_user_prompt_score < specific_label_thresholds.get(final_user_prompt_category, 1.0) and user_prompt_general_toxicity < general_toxicity_threshold:
            final_user_prompt_category = "non_toxic"

        # grok reply - pick the highest scoring label across all specific labels
        grok_reply_label_scores = {
            label: float(grok_reply_toxicity_scores.get(label, 0.0))
            for label in specific_label_thresholds
        }
        logger.debug(f"Grok reply label scores for {conversation_id}/{thread_id}: {grok_reply_label_scores}")
        
        final_grok_reply_category = max(grok_reply_label_scores, key=grok_reply_label_scores.get)
        final_grok_reply_score = grok_reply_label_scores[final_grok_reply_category]

        # Check if the score meets the threshold for that label or the general toxicity threshold
        grok_reply_general_toxicity = float(grok_reply_toxicity_scores.get('toxicity', 0.0))
        if final_grok_reply_score < specific_label_thresholds.get(final_grok_reply_category, 1.0) and grok_reply_general_toxicity < general_toxicity_threshold:
            final_grok_reply_category = "non_toxic"

        return {
            'conversationId': conversation_id,
            'threadId': thread_id,
            'user_prompt': user_prompt,
            'grok_reply': grok_reply,
            'language': language,
            'user_prompt_toxicity_score': final_user_prompt_score,
            "grok_reply_toxicity_score": final_grok_reply_score,
            'user_prompt_category': final_user_prompt_category,
            "grok_reply_category": final_grok_reply_category
        }

    def _get_individual_replies_for_task(self, conversation: Dict) -> List[Dict]:
        replies_to_analyze = []
        conversation_id = conversation.get('conversationId')

        if 'threads' in conversation:
            for threads in conversation.get('threads', []):
                for thread in threads.get('tweets', []):
                    author = thread.get("authorName", '')
                    text = thread.get('text', '')

                    if not author or not text:
                        continue

                    # check for Grok/Assistant authors
                    if author in ["<ASSISTANT>", "Grok", "ASSISTANT"]:
                        replies_to_analyze.append({
                            'role': 'assistant',
                            'reply': text,
                            'threadId': thread.get('threadId', 'N/A'),
                            'conversationId': conversation_id
                        })
        return replies_to_analyze

    def _get_immediate_user_message(self, conversation: Dict) -> List[Dict]:
        user_grok_pairs = []
        conversation_id = conversation.get('conversationId')

        if 'threads' in conversation:
            for threads in conversation.get('threads', []):
                tweets = threads.get('tweets', [])
                
                for i, tweet in enumerate(tweets):
                    author = tweet.get('authorName', '')

                    if not author:
                        continue
                    
                    # When we find a Grok reply
                    if author in ["<ASSISTANT>", "Grok", "ASSISTANT"]:
                        # Get grok response
                        grok_reply = tweet.get('text', '')
                        # Get the immediate previous user message
                        user_prompt = None
                        if i > 0:
                            prev_tweet = tweets[i-1]
                            prev_author = prev_tweet.get('authorName', '')
                            if prev_author not in ["<ASSISTANT>", "Grok", "ASSISTANT"]:
                                user_prompt = prev_tweet.get('text', '')
                        
                        user_grok_pairs.append({
                            'user_prompt': user_prompt,
                            'grok_reply': grok_reply,
                            'threadId': threads.get('threadId', 'N/A'),
                            'conversationId': conversation_id
                        })
        
        return user_grok_pairs
