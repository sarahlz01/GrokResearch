from datetime import datetime
import logging
import os
import traceback
from typing import Dict, List

from batch import BatchManager
from cache import AnalysisCache
from config import AnalysisConfig
from google import genai
from prompt import Prompts
from storage import EncodingHandler

logger = logging.getLogger(__name__)

class DiscussionAnalyzer:
    """Analyzes discussion in conversations"""

    def __init__(self, config: AnalysisConfig, cache: AnalysisCache, prompts: Prompts, storage: EncodingHandler, batch: BatchManager):
        logger.info("Initializing DiscussionAnalyzer")
        self.config = config
        self.cache = cache
        self.prompts = prompts
        self.storage = storage
        self.batch = batch

        self.max_retries = self.config.max_retries
        logger.debug(f"configured max retries: {self.max_retries}")

        # Configure Gemini API
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            logger.error("GEMINI_API_KEY environment variable not set")
            raise ValueError("GEMINI_API_KEY environment variable not set. Please create a .env file and add it.")
        
        logger.debug("gemini api key found, initializing client")
        self.client = genai.Client(api_key=api_key)

        logger.info(f"Primary model: {self.config.model}")
        logger.info(f"Max retries: {self.config.max_retries}")
        logger.info("DiscussionAnalyzer initialization complete")

    def analyze_batch(self, conversations: List[Dict], chunk_id: int) -> List[Dict]:
        """
        Performs two-step analysis for conversations.
        Analyses conversations tagged as discussions in detail.
        """
        logger.info(f"starting batch analysis for chunk {chunk_id}")
        logger.debug(f"received {len(conversations) if conversations else 0} conversations")
        
        try:
            final_results = []
            failed_conversations = {}
            
            # Step 1: Discussion Detection (all conversations)
            logger.info(f"step 1: checking if conversations are discussions for chunk {chunk_id}")
            discussion_detection_result = self._analyze_conversations_for_discussion(conversations, chunk_id)

            if not discussion_detection_result:
                logger.error(f"discussion detection analysis returned None for chunk {chunk_id}")
                # return empty list consistently on hard failure
                return []

            logger.info(f"received {len(discussion_detection_result)} intent analysis results")

            # build map of all results (both yes, no and uncertain)
            intent_map = {}
            detailed_map = {}
            discussion_conversation_ids = set()  # only "yes" responses for detailed analysis

            for result in discussion_detection_result:
                conv_id = result.get("conversationId")
                is_discussion = result.get("is_discussion", "").lower()
                
                if not conv_id:
                    logger.warning(f"found result with null conversation_id in chunk {chunk_id}")
                    continue
                
                # store all results
                intent_map[conv_id] = result
                
                # track only discussion tagged conversations for detailed analysis
                if is_discussion == "yes":
                    discussion_conversation_ids.add(conv_id)

            logger.info(f"processed {len(intent_map)} total conversations for discussion detection")
            logger.info(f"identified {len(discussion_conversation_ids)} conversations as discussions")

            # build conversation lookup for all conversations
            conversation_map = {
                conv.get('conversationId'): conv
                for conv in conversations
                if conv.get('conversationId')
            }
            
            logger.debug(f"built conversation map with {len(conversation_map)} entries")

            # handle failed conversations
            logger.info(f"these ids failed in batch run: {[id for id in failed_conversations]}")

            # validate mapping
            missing_conversations = intent_map.keys() - conversation_map.keys()
            if missing_conversations:
                logger.error(
                    f"key mismatch: {len(missing_conversations)} conversation IDs from analysis "
                    f"not found in original conversations: {list(missing_conversations)[:5]}"
                )

            # Step 2: Discussion Analysis (only for discussion tagged conversations) ---            
            if discussion_conversation_ids:
                # filter to only discussion detailed analysis
                discussion_conversations = [
                    conv for conv in conversations 
                    if conv.get('conversationId') in discussion_conversation_ids
                ]
                
                logger.info(f"filtered to {len(discussion_conversations)} conversations for discussion analysis")
                
                if discussion_conversations:
                    logger.info(f"step 2: performing detailed analysis for {len(discussion_conversations)} discussion conversations")
                    detailed_result = self._analyze_discussion_conversations(discussion_conversations, chunk_id)

                    if detailed_result:
                        detailed_map = {
                            result.get('conversationId'): result 
                            for result in detailed_result
                            if result.get('conversationId')
                        }
                        logger.info(f"created detailed map with {len(detailed_map)} entries")
                    else:
                        logger.warning(f"no detailed analysis results for chunk {chunk_id}")
                else:
                    logger.error(f"conversation filtering failed: {len(discussion_conversation_ids)} ids but 0 conversations matched")
            else:
                logger.info(f"no discussion detected in chunk {chunk_id}, skipping detailed analysis")
            
            # --- Step 3: Merge ALL results ---
            for conv_id, intent_result in intent_map.items():
                conversation = conversation_map.get(conv_id)
                
                if not conversation:
                    logger.warning(f"skipping {conv_id}: not found in original conversations")
                    continue
                
                is_discussion = intent_result.get("is_discussion", "").lower()
                
                combined = {
                    'conversationId': conv_id,
                    'original_conversation': conversation,
                    'discussion_analysis': {
                        'intent': intent_result.copy()
                    }
                }
                
                # add detailed analysis only if this was a discussion
                if is_discussion == "yes":
                    if conv_id in detailed_map:
                        logger.info(f"adding detailed analysis for discussion {conv_id}")
                        combined['discussion_analysis']['detailed'] = detailed_map[conv_id]
                    else:
                        logger.warning(f"discussion {conv_id} missing detailed analysis")
                        combined['discussion_analysis']['detailed'] = None
                else:
                    # explicitly mark non-discussions conversations
                    combined['discussion_analysis']['detailed'] = None
                    logger.debug(f"conversation {conv_id} marked as non-discussions, no detailed analysis")
                
                final_results.append(combined)
            
            logger.info(
                f"batch analysis complete for chunk {chunk_id}: "
                f"{len(final_results)} total results "
                f"({len(discussion_conversation_ids)} discussions, {len(final_results) - len(discussion_conversation_ids)} non-discussions)"
            )
            
            return final_results

        except Exception as e:
            logger.error(f"batch analysis failed for chunk {chunk_id}: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            return []  # consistent return type on error
        
    def _analyze_discussion_conversations(self, conversations: List[Dict], chunk_id: int) -> List[Dict]:
        """Analyzes a discussion tagged conversation and assistants response in detail."""
        failed_conversations = {}
        
        logger.info(f"starting detailed discusison analysis for chunk {chunk_id}")
        logger.debug(f"analyzing {len(conversations) if conversations else 0} conversations")
        
        logger.info(f"Creating detailed analysis prompt")
        prompt = self.prompts._create_discussion_analysis_prompt()
        logger.debug(f"prompt created, length: {len(prompt) if prompt else 0} characters")
        
        try:
            logger.info(f"running batch pipeline for discussion analysis on chunk {chunk_id}")
            batch_results = self.batch.run_batch_pipeline(conversations, chunk_id, prompt)
            
            if not batch_results:
                logger.warning(f"batch pipeline returned no results for chunk {chunk_id}")
                return None
            
            logger.debug(f"batch pipeline complete, received {len(batch_results) if isinstance(batch_results, list) else 'non-list'} results")
            
            logger.info(f"parsing discussion analysis response for chunk {chunk_id}")
            discussion_analysis_results, failure_info = self.storage._parse_discussion_analysis_response(batch_results)

            conversation_map = {conv['conversationId']: conv for conv in conversations}

            if failure_info:
                for conversation_id, failure_reason in failure_info.items():
                    conversation = conversation_map.get(conversation_id)                  
                    failed_conversations[conversation_id] = {
                        'conversation': conversation,
                        'metadata': {
                            'stage': 'discussion_analysis',
                            'chunk': chunk_id,
                            'timestamp': datetime.now().isoformat(),
                            'failure_reason': failure_reason
                    }
                }

            if failed_conversations:
                self.storage.save_failed_conversations(failed_conversations)
            
            logger.info(f"discussion analysis parsing complete, extracted {len(discussion_analysis_results) if isinstance(discussion_analysis_results, list) else 'non-list'} analysis data")
                
            return discussion_analysis_results
        
        except Exception as e:
            logger.error(f"discussion analysis failed for chunk {chunk_id}: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return None
    
    
    def _analyze_conversations_for_discussion(self, conversations: List[Dict], chunk_id: int) -> List[Dict]:
        """Analyzes if a user's tweet contains discussion detection."""
        failed_conversations = {}
        
        logger.info(f"starting discussion detection analysis for chunk {chunk_id}")
        logger.debug(f"analyzing {len(conversations) if conversations else 0} conversations")
        
        logger.info(f"creating discussion detection prompt")
        prompt = self.prompts._create_discussion_detection_prompt()
        logger.debug(f"prompt created, length: {len(prompt) if prompt else 0} characters")
        
        try:
            logger.info(f"running batch pipeline for discussion detection on chunk {chunk_id}")
            batch_results = self.batch.run_batch_pipeline(conversations, chunk_id, prompt)
            
            if not batch_results:
                logger.warning(f"batch pipeline returned no results for discussion detection on chunk {chunk_id}")
                return []
            
            logger.debug(f"batch pipeline complete, received {len(batch_results) if isinstance(batch_results, list) else 'non-list'} results")
            
            logger.info(f"parsing discussion detection response for chunk {chunk_id}")
            discussion_detection_result, failure_info = self.storage._parse_discussion_detection_response(batch_results)

            conversation_map = {conv['conversationId']: conv for conv in conversations}

            if failure_info:
                for conversation_id, failure_reason in failure_info.items():
                    conversation = conversation_map.get(conversation_id)                  
                    failed_conversations[conversation_id] = {
                        'conversation': conversation,
                        'metadata': {
                            'stage': 'discussion_detection',
                            'chunk': chunk_id,
                            'timestamp': datetime.now().isoformat(),
                            'failure_reason': failure_reason
                    }
                }

            if failed_conversations:
                self.storage.save_failed_conversations(failed_conversations)
            
            logger.info(f"discussion detection analysis complete: identified {len(discussion_detection_result) if isinstance(discussion_detection_result, list) else 'non-list'} results")
            return discussion_detection_result
        
        except Exception as e:
            logger.error(f"discussion detection analysis failed for chunk {chunk_id}: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return []