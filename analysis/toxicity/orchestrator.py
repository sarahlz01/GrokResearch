import asyncio
import logging
import time
from collections import Counter

import ijson
from build_analysis import ToxicityAnalyzer
from config import AnalysisConfig
from storage import EncodingHandler
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, config: AnalysisConfig, toxicity_analyzer: ToxicityAnalyzer, storage: EncodingHandler):
        self.config = config
        self.file_path = self.config.file_path
        self.output_path = self.config.output_path
        self.number_of_chunks = self.config.number_of_chunks

        self.toxicity_analyzer = toxicity_analyzer
        self.storage = storage


    async def analyze_grok_replies(self, chunk_id: int = None):
        try:
            start_time = time.time()
            logger.info(f"Starting analysis. Streaming data from {self.file_path}....")

            all_reply_tasks = []
            conversations_prepared = 0

            with open(self.file_path, 'rb') as f:
                conversations = ijson.items(f, 'item')

                for i, conversation in enumerate(tqdm(conversations, desc="Preparing Tasks")):
                    conversation_id = conversation.get('conversationId')

                    if chunk_id is not None and i % self.number_of_chunks != (chunk_id - 1):
                        continue

                    # replies = self.toxicity_analyzer._get_individual_replies_for_task(conversation)
                    
                    # get toxic replies for all tweets (users and grok)
                    replies = self.toxicity_analyzer._get_immediate_user_message(conversation)

                    if replies:
                        for reply_data in replies:
                            task = asyncio.create_task(self.toxicity_analyzer.analyze_single_reply(reply_data))
                            all_reply_tasks.append(task)
                        conversations_prepared += 1

            logger.info(f"Prepared {len(all_reply_tasks)} individual reply analysis tasks from {conversations_prepared} conversations.")

            all_results = []
            for future in tqdm(asyncio.as_completed(all_reply_tasks), total=len(all_reply_tasks), desc="Executing Concurrent Predictions"):
                result = await future

                # if result and result.get('category') != 'prediction_error':
                if result and result.get('user_prompt_category') != 'prediction_error' and result.get('grok_reply_category') != 'prediction_error':
                    all_results.append(result)

            end_time = time.time()
            total_duration = end_time - start_time
            successful_predictions = len(all_results)

            summary = self.generate_summary(all_results)

            output = {
                "summary": summary,
                "reply_analysis": all_results,
                "metadata": {
                    "total_conversations_prepared": conversations_prepared,
                    "total_replies_processed": successful_predictions,
                    "total_duration_seconds": round(total_duration, 2),
                    "throughput_replies_per_second": round(successful_predictions / total_duration, 2) if total_duration > 0 else 0.0,
                }
            }

            # save results
            self.storage.save_json_with_encoding(output, chunk_id=chunk_id)
            logger.info(f"Full analysis complete. Results saved to {self.output_path}")

            return output

        except Exception as e:
            logger.error(f"Analysis Failed: {e}")
            return {}
        
    def generate_summary(self, results: list) -> dict:
        total_replies = len(results)

        toxic_user_prompts = [r for r in results if r['user_prompt_category'] != 'non_toxic']
        non_toxic_user_prompts = [r for r in results if r['user_prompt_category'] == 'non_toxic']
        toxic_grok_replies = [r for r in results if r['grok_reply_category'] != 'non_toxic']
        non_toxic_grok_replies = [r for r in results if r['grok_reply_category'] == 'non_toxic']

        user_prompt_category_counts = Counter(r['user_prompt_category'] for r in toxic_user_prompts)
        grok_reply_category_counts = Counter(r['grok_reply_category'] for r in toxic_grok_replies)

        toxic_user_prompt_percentage = (len(toxic_user_prompts) / total_replies) * 100 if total_replies > 0 else 0
        non_toxic_user_prompt_percentage = (len(non_toxic_user_prompts) / total_replies) * 100 if total_replies > 0 else 0
        toxic_grok_reply_percentage = (len(toxic_grok_replies) / total_replies) * 100 if total_replies > 0 else 0
        non_toxic_grok_reply_percentage = (len(non_toxic_grok_replies) / total_replies) * 100 if total_replies > 0 else 0

        summary = {
            "total_replies": total_replies,
            "user_prompt": {
                "toxic": len(toxic_user_prompts),
                "non_toxic": len(non_toxic_user_prompts),
                "toxic_percentage": round(toxic_user_prompt_percentage, 2),
                "non_toxic_percentage": round(non_toxic_user_prompt_percentage, 2),
                "category_distribution": dict(user_prompt_category_counts),
            },
            "grok_reply": {
                "toxic": len(toxic_grok_replies),
                "non_toxic": len(non_toxic_grok_replies),
                "toxic_percentage": round(toxic_grok_reply_percentage, 2),
                "non_toxic_percentage": round(non_toxic_grok_reply_percentage, 2),
                "category_distribution": dict(grok_reply_category_counts),
            },
        }

        return summary
