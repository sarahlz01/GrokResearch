import argparse
import logging
import sys
import time
from pathlib import Path

from batch import BatchManager
from build_analysis import DiscussionAnalyzer
from cache import AnalysisCache
from config import AnalysisConfig, load_config
from dotenv import load_dotenv
from orchestrator import Orchestrator
from prompt import Prompts
from storage import EncodingHandler

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--chunk-id", 
        type=int, 
        default=None,
        help="Process a specific parallel chunk (1 to N)."
    )
    
    return parser.parse_args()



def load_and_filter_conversations(storage: EncodingHandler, config: AnalysisConfig) -> list:
    """Load conversations from file and apply chunking if needed."""
    logger.info(f"Loading conversations from: {config.file_path}")
    all_conversations = storage.load_json_with_encoding()

    args = parse_arguments()
    
    if not all_conversations:
        logger.error("No conversations loaded from file")
        sys.exit(1)
    
    total_loaded = len(all_conversations)
    logger.info(f"Loaded {total_loaded} conversations from file")
    
    # Apply chunking if chunk_id is set
    if args.chunk_id is not None:
        chunk_size = total_loaded // config.number_of_chunks
        start_idx = (args.chunk_id - 1) * chunk_size
        
        # Last chunk gets any remainder
        if args.chunk_id == config.number_of_chunks:
            end_idx = total_loaded
        else:
            end_idx = start_idx + chunk_size
        
        conversations = all_conversations[start_idx:end_idx]
        logger.info(
            f"Chunk {args.chunk_id}: Processing conversations "
            f"{start_idx} to {end_idx-1} ({len(conversations)} total)"
        )
    else:
        conversations = all_conversations
        logger.info("Processing all conversations (no chunking)")
    
    return conversations


def main():
    """Main entry point for the disucssion analysis."""
    logger.info("DISCUSSION ANALYSIS PIPELINE - STARTING")
    
    script_start = time.time()
    
    try:
        # parse arguments
        args = parse_arguments()
        
        # load and validate configuration
        logger.info("Loading configuration...")
        config = load_config()

        if not config.file_path:
            logger.error("Configuration error: file_path is not set. Please set FILE_PATH_DISCUSSION in .env")
            sys.exit(2)
        if not config.output_path:
            logger.error("Configuration error: output_path is not set. Please set OUTPUT_PATH_DISCUSSION in .env")
            sys.exit(2)

        # check file exists
        if not Path(config.file_path).exists():
            logger.error(f"Input file does not exist: {config.file_path}")
            sys.exit(2)
        
        logger.info("Configuration loaded:")
        logger.info(f"Input file: {config.file_path}")
        logger.info(f"Output path: {config.output_path}")
        logger.info(f"Model: {config.model}")
        logger.info(f"Max conversations: {config.max_conversations or 'All'}")
        logger.info(f"Chunk ID: {args.chunk_id or 'None'}")
        logger.info(f"Number of chunks: {config.number_of_chunks}")
        logger.info(f"Cache enabled: {config.clear_cache}")
        
        # initialize components
        logger.info("Initializing components...")
        cache = AnalysisCache(config)

        chunk_id = args.chunk_id
        
        if config.clear_cache:
            logger.info("Clearing cache as requested...")
            cache.clear_cache()
        
        prompts = Prompts()
        storage = EncodingHandler(config, cache)
        batch = BatchManager(config, prompts)
        
        discussion_analyzer = DiscussionAnalyzer(config, cache, prompts, storage, batch)
        orchestrator = Orchestrator(config, storage, discussion_analyzer, cache)
        
        # Load and filter conversations
        conversations = load_and_filter_conversations(storage, config)
        
        # Run analysis
        logger.info("Starting analysis...")
        result = orchestrator.analyze_discussion_interactions(conversations, chunk_id)
        
        # Report success
        total_time = time.time() - script_start
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Total execution time: {total_time:.2f}s")
        logger.info(f"Processed {result['metadata']['processed_conversations']} conversations")
        logger.info(f"Detected {result['metadata']['discussion_conversations_detected']} discussions")
        
        return 0
        
    except KeyboardInterrupt:
        total_time = time.time() - script_start
        logger.warning(f"PIPELINE INTERRUPTED BY USER after {total_time:.2f}s")
        return 130
        
    except Exception as e:
        total_time = time.time() - script_start
        logger.error(f"PIPELINE FAILED after {total_time:.2f}s")
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())