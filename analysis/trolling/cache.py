import hashlib
import json
import logging
import pickle
import traceback
from typing import Dict, Optional

from config import AnalysisConfig

logger = logging.getLogger(__name__)

class AnalysisCache:
    """Cache for analysis results to avoid duplicate API calls."""

    def __init__(self, config: AnalysisConfig):
        self.config = config

        self.cache_dir = self.config.cache_dir
        self.cache_dir.mkdir(exist_ok=True)

        self.cache_enabled = self.config.cache_enabled
        logger.info(f"Initialized cache directory: {self.cache_dir}")

    def _get_cache_key(self, conversation: Dict, analysis_type: str) -> str:
        """Generate a unique cache key for an interaction pair."""
        content_str = json.dumps(conversation, sort_keys=True)
        return hashlib.md5(f"{content_str}_{analysis_type}".encode()).hexdigest()

    def get(self, conversation: Dict, analysis_type: str) -> Optional[Dict]:
        """Get a cached result for an interaction pair."""
        if not self.cache_enabled:
            logger.debug("Cache is disabled, skipping lookup")
            return None
        
        cache_key = self._get_cache_key(conversation, analysis_type)
        cache_file = self.cache_dir / f"{cache_key}.pkl"

        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                    logger.debug(f"Cache hit for {analysis_type} (key: {cache_key[:8]}...)")
                    return cached_data
            except Exception as e:
                logger.warning(f"Failed to load cache file {cache_key[:8]}...: {e}")
                logger.debug(f"Traceback: {traceback.format_exc()}")
                cache_file.unlink(missing_ok=True)
                logger.info(f"Removed corrupted cache file")
        else:
            logger.debug(f"Cache miss for {analysis_type} (key: {cache_key[:8]}...)")
        return None

    def set(self, conversation: Dict, analysis_type: str, result: Dict):
        """Cache a result for an interaction pair."""
        if not self.cache_enabled:
            logger.debug("Cache is disabled, skipping write")
            return

        cache_key = self._get_cache_key(conversation, analysis_type)
        cache_file = self.cache_dir / f"{cache_key}.pkl"

        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
            logger.debug(f"Cached result for {analysis_type} (key: {cache_key[:8]}...)")
        except Exception as e:
            logger.warning(f"Failed to cache result {cache_key[:8]}...: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")

    def clear_cache(self):
        """Clear all cached results."""
        logger.info("Clearing cache...")
        if self.cache_dir.exists():
            cache_files = list(self.cache_dir.glob("*.pkl"))
            file_count = len(cache_files)
            for cache_file in cache_files:
                cache_file.unlink()
            logger.info(f"Cache cleared: removed {file_count} files")
        else:
            logger.warning("Cache directory does not exist")