import logging
import time
import traceback
from typing import Dict, List

from build_analysis import TrollAnalyzer
from cache import AnalysisCache
from config import AnalysisConfig
from storage import EncodingHandler

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self, config: AnalysisConfig, storage: EncodingHandler, troll_analyzer: TrollAnalyzer, cache: AnalysisCache):
        self.config = config
        self.cache = cache
        self.cache_enabled = self.cache.cache_enabled

        self.max_conversations = self.config.max_conversations
        self.model = self.config.model

        self.storage = storage
        self.troll_analyzer = troll_analyzer
        
        logger.info("Orchestrator initialized")

    def analyze_troll_interactions(self, conversations: List[Dict], chunk_id: int) -> Dict:
        """Orchestrate the entire analysis workflow."""
        logger.info("STARTING TROLL ANALYSIS")
        logger.info(f"Conversations to process: {len(conversations)}")
        logger.info(f"Max conversations limit: {self.max_conversations if self.max_conversations else 'None'}")
        logger.info(f"Chunk ID: {chunk_id if chunk_id is not None else 'None (full dataset)'}")
        logger.info(f"Cache enabled: {self.cache_enabled}")
        
        start_time = time.time()
        
        try:
            # Apply max_conversations limit if set
            if self.max_conversations and len(conversations) > self.max_conversations:
                logger.info(f"Limiting to first {self.max_conversations} conversations")
                conversations = conversations[:self.max_conversations]
            
            total_conversations = len(conversations)
            logger.info(f"Processing {total_conversations} conversations...")
            
            # run batch analysis
            analysis_results = self.troll_analyzer.analyze_batch(
                conversations, 
                chunk_id if chunk_id is not None else 0
            )
            
            if not analysis_results:
                logger.error("Analysis returned empty results")
                raise ValueError("Analysis produced no results")
            
            # count trolling detections
            trolling_conversations_detected = sum(
                1 for result in analysis_results 
                if result.get('trolling_analysis', {}).get('intent', {}).get('is_trolling') == 'yes'
            )
            
            # count successful processing
            successfully_processed = len(analysis_results)
            failed_conversations = total_conversations - successfully_processed
            
            processing_time = time.time() - start_time
            
            logger.info("ANALYSIS SUMMARY")
            logger.info(f"Total conversations: {total_conversations}")
            logger.info(f"Successfully processed: {successfully_processed}")
            logger.info(f"Failed: {failed_conversations}")
            logger.info(f"Trolling detected: {trolling_conversations_detected} ({trolling_conversations_detected/total_conversations*100:.1f}%)")
            logger.info(f"Total processing time: {processing_time:.2f}s")
            logger.info(f"Average time per conversation: {processing_time/total_conversations:.2f}s")
            
            # build metadata
            metadata = {
                "total_conversations": total_conversations,
                "processed_conversations": successfully_processed,
                "failed_conversations": failed_conversations,
                "trolling_conversations_detected": trolling_conversations_detected,
                "trolling_rate": round(trolling_conversations_detected / total_conversations, 4) if total_conversations > 0 else 0,
                "processing_time_seconds": round(processing_time, 2),
                "avg_processing_time_per_conversation": round(processing_time / total_conversations, 2) if total_conversations > 0 else 0,
                "config": {
                    "model": self.model,
                    "max_conversations": self.max_conversations,
                    "cache_enabled": self.cache_enabled,
                    "chunk_id": chunk_id
                }
            }
            
            # prepare final output
            output = {
                "analysis_results": analysis_results,
                "metadata": metadata
            }

            # save results
            self.storage.save_json_with_encoding(output, chunk_id)
            
            logger.info(f"ANALYSIS COMPLETE")            
            return output

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"ANALYSIS FAILED after {elapsed:.2f}s")
            logger.error(f"Error: {e}")
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            raise