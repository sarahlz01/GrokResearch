import codecs
import json
import logging
import re
import traceback
from pathlib import Path
from typing import Dict, List

import chardet
from config import AnalysisConfig

logger = logging.getLogger(__name__)


class EncodingHandler:
    """Handles storage, different text encodings for file operations and mojibake repair."""

    def __init__(self, config: AnalysisConfig):
        self.config = config

        self.file_path = Path(self.config.file_path)
        self.output_path = Path(self.config.output_path)

        self.output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"initialized encoding handler with file: {self.file_path}")
        logger.info(f"output directory: {self.output_path}")

    def detect_encoding(self) -> str:
        """Detect the encoding of a file."""
        try:
            logger.debug(f"detecting encoding for file: {self.file_path}")
            with open(self.file_path, 'rb') as file:
                raw_data = file.read()
                result = chardet.detect(raw_data)
                detected_encoding = result['encoding'] or 'utf-8'
                logger.debug(f"detected encoding '{detected_encoding}' with confidence {result.get('confidence', 0):.2f}")
                return detected_encoding
        except Exception as e:
            logger.warning(f"could not detect encoding for {self.file_path}: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            return 'utf-8'

    def repair_mojibake(self, text: str) -> str:
        """Fix UTF-8 that was mis-decoded as Windows-1252/Latin-1."""
        try:
            return text.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

    def repair_dict(self, obj):
        """Recursively repair strings inside JSON-like structures."""
        if isinstance(obj, dict):
            return {k: self.repair_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.repair_dict(i) for i in obj]
        elif isinstance(obj, str):
            return self.repair_mojibake(obj)
        return obj

    def load_json_with_encoding(self) -> Dict:
        """Load JSON file with proper encoding detection and repair mojibake."""
        logger.info(f"loading json file: {self.file_path}")
        encoding = self.detect_encoding()
        
        try:
            with codecs.open(self.file_path, 'r', encoding=encoding) as file:
                data = json.load(file)
                logger.info(f"successfully loaded json with encoding: {encoding}")
                repaired_data = self.repair_dict(data)
                logger.debug(f"repaired mojibake in loaded data")
                return repaired_data
        except UnicodeDecodeError:
            logger.warning(f"failed to load {self.file_path} with detected encoding {encoding}, falling back to utf-8")
            try:
                with codecs.open(self.file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    logger.info("successfully loaded json with utf-8 fallback")
                    return self.repair_dict(data)
            except Exception as e:
                logger.error(f"failed to load {self.file_path} with utf-8: {e}")
                logger.debug(f"traceback: {traceback.format_exc()}")
                raise
        except Exception as e:
            logger.error(f"failed to load {self.file_path}: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            raise

    def save_json_with_encoding(self, data: Dict, chunk_id: int, encoding: str = 'utf-8'):
        """Save JSON file with specified encoding."""
        filename = f"output_raw_chunk_{chunk_id}.json" if chunk_id else f"output_raw_chunk_0.json"
        output_file = self.output_path / filename

        logger.info(f"saving json to {output_file}")
        logger.debug(f"using encoding: {encoding}")
        
        try:
            with codecs.open(output_file, 'w', encoding=encoding) as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
            logger.info(f"successfully saved json file: {output_file}")
        except Exception as e:
            logger.error(f"failed to save {output_file}: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            raise