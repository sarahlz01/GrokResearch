import logging
import time
import traceback
from typing import Dict, List

from build_analysis import DiscussionAnalyzer
from cache import AnalysisCache
from config import AnalysisConfig
from storage import EncodingHandler

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self, config: AnalysisConfig, storage: EncodingHandler, discussion_analyzer: DiscussionAnalyzer, cache: AnalysisCache):
        self.config = config
        self.cache = cache
        self.cache_enabled = self.cache.cache_enabled

        self.max_conversations = self.config.max_conversations
        self.model = self.config.model

        self.storage = storage
        self.discussion_analyzer = discussion_analyzer
        
        logger.info("Orchestrator initialized")                                                 

    def analyze_discussion_interactions(self, conversations: List[Dict], chunk_id: int) -> Dict:
        """Orchestrate the entire analysis workflow."""
        logger.info("STARTING DISCUSSION ANALYSIS")
        logger.info(f"Conversations to process: {len(conversations)}")
        logger.info(f"Max conversations limit: {self.max_conversations if self.max_conversations else 'None'}")
        logger.info(f"Chunk ID: {chunk_id if chunk_id is not None else 'None (full dataset)'}")
        logger.info(f"Cache enabled: {self.cache_enabled}")
        
        start_time = time.time()
        
        try:
            # apply max_conversations limit if set
            if self.max_conversations and len(conversations) > self.max_conversations:
                logger.info(f"Limiting to first {self.max_conversations} conversations")
                conversations = conversations[:self.max_conversations]
            
            total_conversations = len(conversations)
            logger.info(f"Processing {total_conversations} conversations...")
            
            # run batch analysis
            analysis_results = self.discussion_analyzer.analyze_batch(
                conversations, 
                chunk_id if chunk_id is not None else 0
            )
            
            if not analysis_results:
                logger.error("Analysis returned empty results")
                raise ValueError("Analysis produced no results")
            
            # count discussioning detections
            discussion_conversations_detected = sum(
                1 for result in analysis_results 
                if result.get('discussion_analysis', {}).get('intent', {}).get('is_discussion') == 'yes'
            )
            
            # count successful processing
            successfully_processed = len(analysis_results)
            failed_conversations = total_conversations - successfully_processed
            
            processing_time = time.time() - start_time
            
            logger.info("ANALYSIS SUMMARY")
            logger.info(f"Total conversations: {total_conversations}")
            logger.info(f"Successfully processed: {successfully_processed}")
            logger.info(f"Failed: {failed_conversations}")
            logger.info(f"discussion detected: {discussion_conversations_detected} ({discussion_conversations_detected/total_conversations*100:.1f}%)")
            logger.info(f"Total processing time: {processing_time:.2f}s")
            logger.info(f"Average time per conversation: {processing_time/total_conversations:.2f}s")
            
            # build metadata
            metadata = {
                "total_conversations": total_conversations,
                "processed_conversations": successfully_processed,
                "failed_conversations": failed_conversations,
                "discussion_conversations_detected": discussion_conversations_detected,
                "discussion_rate": round(discussion_conversations_detected / total_conversations, 4) if total_conversations > 0 else 0,
                "processing_time_seconds": round(processing_time, 2),
                "avg_processing_time_per_conversation": round(processing_time / total_conversations, 2) if total_conversations > 0 else 0,
                "config": {
                    "model": self.model,
                    "max_conversations": self.max_conversations,
                    "cache_enabled": self.cache_enabled,
                    "chunk_id": chunk_id
                }
            }

            summary = self._generate_summary(analysis_results, chunk_id)
            logger.info("Summary generation complete.")
            
            # prepare final output
            output = {
                "summary": summary,
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

    def _generate_summary(self, analysis_results: List[Dict], chunk_id: int) -> Dict:
        """Generate a comprehensive summary of all processed conversations."""
        total_conversations = len(analysis_results)
        
        # Count successful analyses (those with valid required fields)
        successful_analyses = sum(
            1 for conv in analysis_results 
            if 'discussion_analysis' in conv and 'intent' in conv['discussion_analysis']
        )
        
        # Analyze discussion patterns
        discussion_counts = {'yes': 0, 'no': 0, 'uncertain': 0}
        discussion_intensities = []
        discussion_types = []
        topics = []
        
        # Analyze bias patterns
        bias_language_counts = {'yes': 0, 'no': 0, 'uncertain': 0}
        assistant_stances = []
        user_response_types = []

        
        for conv in analysis_results:
            discussion_analysis = conv.get('discussion_analysis', {})
            intent = discussion_analysis.get('intent', {})
            detailed = discussion_analysis.get('detailed', {})
            
            # Discussion analysis
            is_discussion = intent.get('is_discussion', 'uncertain')
            discussion_counts[is_discussion] = discussion_counts.get(is_discussion, 0) + 1
            
            if detailed:
                if 'discussion_intensity' in detailed and isinstance(detailed['discussion_intensity'], (int, float)):
                    discussion_intensities.append(detailed['discussion_intensity'])
                if 'discussion_type' in detailed and isinstance(detailed['discussion_type'], list):
                    discussion_types.extend(detailed['discussion_type'])
                if 'topic' in detailed:
                    topics.append(detailed['topic'])
                
                # Bias analysis
                if 'bias_language' in detailed and isinstance(detailed['bias_language'], dict):
                    bias_lang = detailed['bias_language'].get('bias_language', 'uncertain')
                    bias_language_counts[bias_lang] = bias_language_counts.get(bias_lang, 0) + 1
                if 'assistant_stance' in detailed and isinstance(detailed['assistant_stance'], dict):
                    stance = detailed['assistant_stance'].get('stance', 'uncertain')
                    assistant_stances.append(stance)
                if 'user_response' in detailed and isinstance(detailed['user_response'], dict):
                    response_type = detailed['user_response'].get('type', 'other')
                    user_response_types.append(response_type)

        # Calculate averages
        avg_discussion_intensity = round(sum(discussion_intensities) / len(discussion_intensities), 2) if discussion_intensities else 0
        
        # Count frequencies
        discussion_type_counts = {}
        for dt in discussion_types:
            discussion_type_counts[dt] = discussion_type_counts.get(dt, 0) + 1
        
        assistant_stance_counts = {}
        for stance in assistant_stances:
            assistant_stance_counts[stance] = assistant_stance_counts.get(stance, 0) + 1
        
        user_response_type_counts = {}
        for response_type in user_response_types:
            user_response_type_counts[response_type] = user_response_type_counts.get(response_type, 0) + 1
        
        return {
            "chunk_id": chunk_id,
            "total_conversations": total_conversations,
            "successful_analyses": successful_analyses,
            "success_rate": round(successful_analyses / total_conversations * 100, 2) if total_conversations > 0 else 0,
            "discussion_analysis": {
                "discussion_counts": discussion_counts,
                "average_discussion_intensity": avg_discussion_intensity,
                "discussion_type_distribution": dict(sorted(discussion_type_counts.items(), key=lambda x: x[1], reverse=True)),
                "common_topics": list(set(topics))[:10]
            },
            "bias_analysis": {
                "bias_language_counts": bias_language_counts,
                "assistant_stance_distribution": dict(sorted(assistant_stance_counts.items(), key=lambda x: x[1], reverse=True)),
                "user_response_type_distribution": dict(sorted(user_response_type_counts.items(), key=lambda x: x[1], reverse=True))
            }
        }

