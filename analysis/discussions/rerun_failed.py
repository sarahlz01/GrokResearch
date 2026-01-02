import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List

import ijson
from batch import BatchManager
from build_analysis import DiscussionAnalyzer
from cache import AnalysisCache
from config import AnalysisConfig
from dotenv import load_dotenv
from orchestrator import Orchestrator
from prompt import Prompts
from storage import EncodingHandler

load_dotenv()

# pass conversation to the orchestartor with chunk_id 0, change path from env to 2025_00

JSONL_PATH = os.getenv("FAILED_PATH_DISCUSSION")
CONVERSATION_PRIME = os.getenv("CONVERSATION_PRIME") # all conversations from march to october

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def extract_jsonl_ids(jsonl_file: Path) -> List[str]:
    conversation_ids = []
    
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                entry = json.loads(line)
                conv_id = entry.get('conversationId')
                
                if conv_id:
                    conversation_ids.append(conv_id)
                else:
                    logger.info(f"Line {line_num}: No conversationId found")
                    
            except json.JSONDecodeError as e:
                logger.info(f"Line {line_num}: Failed to parse JSON - {e}")
                continue
    
    return conversation_ids

def stream_and_filter_conversations(failed_ids: List) -> List[Dict]:
    matched = []
    processed = 0
    with open(CONVERSATION_PRIME, 'rb') as f:
        for item in ijson.items(f, 'item'):
            conv_id = item.get('conversationId')
            threads = item.get('threads')
            processed += 1
            if processed % 10000 == 0:
                logger.info(f"Processed {processed}, matched {len(matched)}")
            if conv_id in failed_ids:
                logger.info(f"Failed ID {conv_id} matched")
                map = {
                    'conversationId': conv_id,
                    'threads': threads
                }
                matched.append(map)
    
    logger.info(f"Complete: {processed} processed, {len(matched)} matched")
    return matched


def main():
    logger.info("TROLL ANALYSIS FAILURE RERUN - STARTING")
    
    script_start = time.time()
    config = AnalysisConfig()
    cache = AnalysisCache(config)
    prompts = Prompts()
    storage = EncodingHandler(config, cache)

    batch = BatchManager(config, prompts, cache)
    discussion_analyzer = DiscussionAnalyzer(config, cache, prompts, storage, batch)
    orchestrator = Orchestrator(config, storage, discussion_analyzer, cache)


    failed_ids = []
    jsonl_ids = extract_jsonl_ids(JSONL_PATH)

    logger.info(f"first failed batch has: {len(jsonl_ids)} conversations")
    failed_ids.extend(jsonl_ids)

    logger.info(f"found {len(failed_ids)} conversation IDs")
    logger.info(f"first 5 IDs: {failed_ids[:5]}")

    # stream and filter data
    failed_conversation_map = stream_and_filter_conversations(failed_ids)
    
    logger.info("starting failure analysis...")
    result = orchestrator.analyze_discussion_interactions(failed_conversation_map, chunk_id=0)
    
    total_time = time.time() - script_start
    logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"Total execution time: {total_time:.2f}s")
    logger.info(f"Processed {result['metadata']['processed_conversations']} conversations")
    logger.info(f"Detected {result['metadata']['discussion_conversations_detected']} discussions")
        

if __name__ == "__main__":
    main()