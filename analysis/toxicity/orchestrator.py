import asyncio
import logging
import time
from collections import Counter

import ijson
from build_analysis import ToxicityAnalzyer
from config import AnalysisConfig
from storage import EncodingHandler
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, config: AnalysisConfig, toxicity_analyzer: ToxicityAnalzyer, storage: EncodingHandler):
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
                    replies = self.toxicity_analyzer._get_immediate_user_message(conversation)

                    if replies:
                        for reply_data in replies:
                            task = self.toxicity_analyzer.analyze_single_reply(reply_data)
                            all_reply_tasks.append(task)
                        conversations_prepared += 1

            logger.info(f"Prepared {len(all_reply_tasks)} individual reply analysis tasks from {conversations_prepared} conversations.")

            all_results = []
            for future in tqdm(asyncio.as_completed(all_reply_tasks), total=len(all_reply_tasks), desc="Executing Concurrent Predictions"):
                result = await future

                if result and result.get('category') != 'prediction_error':
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
        toxic_replies = [r for r in results if r['category'] != 'non_toxic']
        non_toxic_replies = [r for r in results if r['category'] == 'non_toxic']

        category_counts = Counter(r['category'] for r in toxic_replies)

        toxic_percentage = (len(toxic_replies) / total_replies) * 100 if total_replies > 0 else 0
        non_toxic_percentage = (len(non_toxic_replies) / total_replies) * 100 if total_replies > 0 else 0

        summary = {
            "total_replies": total_replies,
            "toxic_replies": len(toxic_replies),
            "non_toxic_replies": len(non_toxic_replies),
            "toxic_percentage": round(toxic_percentage, 2),
            "non_toxic_percentage": round(non_toxic_percentage, 2),
            "category_distribution": dict(category_counts)
        }

        return summary
