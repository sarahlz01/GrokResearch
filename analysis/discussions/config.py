import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from dotenv import load_dotenv

load_dotenv()

CACHE_DIR = os.getenv("CACHE_DIR_DISCUSSION")
FILE_PATH = os.getenv("FILE_PATH_DISCUSSION")
OUTPUT_PATH = os.getenv("OUTPUT_PATH_DISCUSSION")
BATCH_PATH = os.getenv("BATCH_PATH_DISCUSSION")

@dataclass
class AnalysisConfig:
    """Configuration for analysis parameters."""
    model: str = "gemini-2.0-flash"
    fallback_models: List[str] = None  # models to try if primary fails
    max_retries: int = 3
    cache_enabled: bool = True
    cache_dir: str = Path(CACHE_DIR)
    max_conversations: int = None  # maximum conversations to process (None = all)
    job_timeout: int = 7200 # 2 hours timeout
    file_path: Path =  FILE_PATH
    output_path: Path = OUTPUT_PATH
    clear_cache: bool = False
    number_of_chunks: int = 5
    batch_files: str = BATCH_PATH

    if max_retries < 0:
        raise ValueError(f"max_retries must be >= 0")
    

    # fallback_models = [
    #     "gemini-2.5-flash-preview", 
    #     "gemini-2.5-flash-lite", 
    #     "gemini-2.5-flash-lite-preview", 
    #     "gemini-2.5-pro", 
    #     "gemini-2.0-flash-lite",
    #     "gemini-3-pro-preview",
    #     "gemini-2.0-flash"
    # ]
            



def load_config() -> AnalysisConfig:
    """Factory function to create config with defaults."""
    return AnalysisConfig()