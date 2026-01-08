import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

from config import AnalysisConfig
from detoxify import Detoxify
from langdetect import DetectorFactory, detect

logger = logging.getLogger(__name__)


DetectorFactory.seed = 0 # remove this


class ToxicityAnalzyer:
    def __init__(self, config: AnalysisConfig):
        self.config = config

        self.english_model = Detoxify(self.config.english_model_name)
        self.multilingual_model = Detoxify(self.config.multilingual_model_name)
        self.semanphore = asyncio.Semaphore(self.config.max_prediction_concurrency)
        logger.info(f"Initialized ToxicityAnalyzer with models: {self.config.english_model_name} and {self.config.multilingual_model_name}. Concurrency Limit: {self.config.max_prediction_concurrency}")


    async def analyze_single_reply(self, reply_item: dict) -> Dict:
        text = reply_item.get('user_message', '')
        conversation_id = reply_item.get('conversationId')
        thread_id = reply_item.get('threadId', 'N/A')
        role = reply_item.get('role', '')

        if not text:
            return {}

        specific_label_thresholds = {
            'threat': 0.9, 
            'severe_toxicity': 0.9, 
            'identify_attack': 0.9,
            'insult': 0.9, 
            'sexual_explicit': 0.66, 
            'obscene': 0.6,
        }
        general_toxicity_threshold = 0.90

        final_category = ""
        final_score = 0.0

        try:
            language = await asyncio.to_thread(detect, text)
        except Exception:
            language = "unknown"

        if language:
            async with self.semanphore:
                logger.debug(f"Starting prediction. Active predictions: {self.config.max_prediction_concurrency - self.semanphore._value} / {self.config.max_prediction_concurrency}")

                try:
                    if language == "en":
                        prediction = await asyncio.to_thread(self.english_model.predict, text)
                    else:
                        prediction = await asyncio.to_thread(self.multilingual_model.predict, text)
                except Exception as e:
                    logger.error(f"Prediction failed for {conversation_id}/{thread_id}: {e}")
                    return {"conversationId": conversation_id, "threadId": thread_id, "grok_reply": text, "category": "prediction_error", "toxicity_score": 0.0, "role": role}

        toxicity_scores = {k: round(v, 4) for k, v in prediction.items()}
        general_toxicity_score = toxicity_scores.get('toxicity', 0.0)

        if general_toxicity_score >= general_toxicity_threshold:
            final_category = 'toxicity'
            final_score = float(general_toxicity_score)

            for label, threshold in specific_label_thresholds.items():
                current_label_score = float(toxicity_scores.get(label, 0.0))

                if current_label_score >= threshold:
                    final_score = current_label_score
                    final_category = label
                    break
        else:
            final_category = "non_toxic"
            final_score = 0.0

        return {
            'conversationId': conversation_id,
            'threadId': thread_id,
            # 'grok_reply': text,
            'user_message': text,
            'language': language,
            'toxicity_score': final_score,
            'category': final_category,
            'role': role
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
                    text = tweet.get('text', '')
                    
                    # When we find a Grok reply
                    if author in ["<ASSISTANT>", "Grok", "ASSISTANT"]:
                        # Get the immediate previous user message
                        user_msg = None
                        if i > 0:
                            prev_tweet = tweets[i-1]
                            prev_author = prev_tweet.get('authorName', '')
                            if prev_author not in ["<ASSISTANT>", "Grok", "ASSISTANT"]:
                                user_msg = prev_tweet.get('text', '')
                        
                        user_grok_pairs.append({
                            'role': 'user',
                            'user_message': user_msg,
                            'grok_reply': text,
                            'threadId': threads.get('threadId', 'N/A'),
                            'conversationId': conversation_id
                        })
        
        return user_grok_pairs

                     






