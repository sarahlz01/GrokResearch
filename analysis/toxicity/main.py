import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

from build_analysis import ToxicityAnalyzer
from config import load_config
from dotenv import load_dotenv
from orchestrator import Orchestrator
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

def main():

    try: 
        script_start = time.time()
        # parse args
        args = parse_arguments()

        # build config
        logger.info("Loading configuration...")
        config = load_config()

        if not config.file_path:
            logger.error("Configuration error: file_path is not set. Please set FILE_PATH in .env")
            sys.exit(2)
        if not config.output_path:
            logger.error("Configuration error: output_path is not set. Please set OUTPUT_PATH in .env")
            sys.exit(2)

        # check file exists
        if not Path(config.file_path).exists():
            logger.error(f"Input file does not exist: {config.file_path}")
            sys.exit(2)

        logger.info("Configuration loaded:")
        logger.info(f"Input file: {config.file_path}")
        logger.info(f"Output path: {config.output_path}")
        logger.info(f"Chunk ID: {args.chunk_id or 'None'}")
        logger.info(f"Number of chunks: {config.number_of_chunks}")
        

        # initialize components
        chunk_id = args.chunk_id

        storage = EncodingHandler(config)
        toxicity_analyzer = ToxicityAnalyzer(config)
        orchestrator = Orchestrator(config, toxicity_analyzer, storage)

        logger.info("Starting analysis...")

        result = asyncio.run(
            orchestrator.analyze_grok_replies(chunk_id=chunk_id)
        )

        total_time = time.time() - script_start
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Total execution time: {total_time:.2f}s")
        logger.info(f"Total conversations prepared: {result['metadata']['total_conversations_prepared']}")
        logger.info(f"Total replies processed {result['metadata']['total_replies_processed']}")
        logger.info(f"Total Run time: {total_time} ")
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