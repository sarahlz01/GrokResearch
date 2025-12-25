from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

FILE_PATH_TOXIC = os.getenv("FILE_PATH_TOXIC")
OUTPUT_PATH_TOXIC = os.getenv("OUTPUT_PATH_TOXIC")

class OutputPathMissingError(Exception):
    """Raised when expected output directory is not present."""
    pass

@dataclass
class AnalysisConfig:
    """Configuration for analysis parameters."""
    file_path: Path = FILE_PATH_TOXIC
    output_path: Path = OUTPUT_PATH_TOXIC
    english_model_name: str = "original"
    multilingual_model_name: str = "multilingual"
    number_of_chunks: int = 5
    max_prediction_concurrency : int = 15


def load_config() -> AnalysisConfig:
    return AnalysisConfig()
