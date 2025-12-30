import codecs
import json
import logging
import re
import traceback
from pathlib import Path
from typing import Dict, List

import chardet
from config import AnalysisConfig
from cache import AnalysisCache

logger = logging.getLogger(__name__)


class EncodingHandler:
    """Handles storage, different text encodings for file operations and mojibake repair."""

    def __init__(self, config: AnalysisConfig, cache: AnalysisCache):
        self.config = config
        self.cache = cache

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
        filename = f"output_raw_chunk_{chunk_id}.json"
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

    def _parse_troll_detection_response(self, api_response: str) -> List[Dict]:
        """
        Safely parses the JSON from the model's response.
        """
        logger.info("starting troll detection response parsing")
        logger.debug(f"raw response length: {len(api_response) if api_response else 0} characters")
        
        all_analysis = []
        failure_info = {}
        
        try:
            lines = api_response.strip().split("\n")
            logger.info(f"processing {len(lines)} response lines")
            
            for line_num, raw_line in enumerate(lines, 1):
                conversation_id = None  # Initialize to None
                
                if not raw_line.strip():
                    logger.debug(f"line {line_num}: empty, skipping")
                    continue
            
                # parse the batch response line
                try:
                    obj = json.loads(raw_line)
                    conversation_id = obj.get("key")
                    logger.debug(f"line {line_num}: processing conversation {conversation_id}")
                except json.JSONDecodeError as e:
                    logger.error(f"line {line_num}: malformed json - {e}")
                    continue
            
                if not isinstance(obj, dict) or "key" not in obj:
                    logger.error(f"line {line_num}: missing 'key' field, got type {type(obj)}")
                    if conversation_id:
                        failure_info[conversation_id] = "Missing 'key' field in response object"
                    continue

                # extract response text
                try:
                    response_obj = obj["response"]
                    text = response_obj["candidates"][0]["content"]["parts"][0]["text"]
                    logger.debug(f"conversation {conversation_id}: extracted response text ({len(text)} chars)")
                except (KeyError, IndexError, TypeError) as e:
                    logger.error(f"conversation {conversation_id}: could not extract response text - {e}")
                    failure_info[conversation_id] = f"Failed to extract response text: {str(e)}"
                    continue
            
                # clean and parse JSON
                cleaned_text = text.strip().replace("```json", "").replace("```", "")
                
                match = re.search(r"\{.*\}", cleaned_text, re.DOTALL)
                if match:
                    json_str = match.group(0)
                    logger.debug(f"conversation {conversation_id}: extracted json via regex")
                else:
                    json_str = cleaned_text
                    logger.debug(f"conversation {conversation_id}: using full cleaned text as json")

                try:
                    parsed_response = json.loads(json_str)
                    logger.debug(f"conversation {conversation_id}: successfully parsed json response")
                except json.JSONDecodeError as e:
                    logger.error(f"conversation {conversation_id}: failed to parse json - {e}")
                    logger.debug(f"problematic json string: {json_str[:200]}")
                    failure_info[conversation_id] = f"JSON decode error in response content: {str(e)}"
                    continue

                # validate parsed response structure
                if not isinstance(parsed_response, dict):
                    logger.error(f"conversation {conversation_id}: expected dict, got {type(parsed_response)}")
                    failure_info[conversation_id] = f"Expected dict, got {type(parsed_response).__name__}"
                    continue

                # extract and validate is_trolling field
                is_trolling = parsed_response.get('is_trolling', '').lower()
                if is_trolling not in ['yes', 'no', 'uncertain']:
                    logger.warning(
                        f"conversation {conversation_id}: invalid is_trolling value '{is_trolling}', treating as 'no'"
                    )
                    is_trolling = 'no'
                else:
                    logger.debug(f"conversation {conversation_id}: is_trolling = {is_trolling}")

                result = {
                    "conversationId": conversation_id,
                    'is_trolling': is_trolling,
                    'trolling_confidence': parsed_response.get('trolling_confidence'),
                    'trolling_intensity': parsed_response.get('trolling_intensity'),
                    'topic': parsed_response.get('topic', '')
                }

                # validate numeric fields
                if result['trolling_confidence'] is not None:
                    try:
                        result['trolling_confidence'] = int(result['trolling_confidence'])
                        if not (0 <= result['trolling_confidence'] <= 5):
                            logger.warning(
                                f"conversation {conversation_id}: confidence {result['trolling_confidence']} "
                                f"out of range [0-5], setting to None"
                            )
                            result['trolling_confidence'] = None
                    except (ValueError, TypeError):
                        logger.warning(f"conversation {conversation_id}: invalid confidence value, setting to None")
                        result['trolling_confidence'] = None

                if result['trolling_intensity'] is not None:
                    try:
                        result['trolling_intensity'] = int(result['trolling_intensity'])
                        if not (0 <= result['trolling_intensity'] <= 5):
                            logger.warning(
                                f"conversation {conversation_id}: intensity {result['trolling_intensity']} "
                                f"out of range [0-5], setting to None"
                            )
                            result['trolling_intensity'] = None
                    except (ValueError, TypeError):
                        logger.warning(f"conversation {conversation_id}: invalid intensity value, setting to None")
                        result['trolling_intensity'] = None

                all_analysis.append(result)
                logger.debug(f"conversation {conversation_id}: added to results list")
                
                # cache the complete result
                try:
                    if self.cache.set(conversation_id, "trolling_detection", result):
                        logger.debug(f"conversation {conversation_id}: cached successfully")
                except Exception as e:
                    logger.warning(f"conversation {conversation_id}: failed to cache - {e}")
            
            logger.info(f"troll detection parsing complete: {len(all_analysis)} successful, {len(failure_info)} failed")
            
            if len(failure_info) > 0:
                logger.warning(f"parsing had {len(failure_info)} failures out of {len(lines)} total lines")
            
            return all_analysis, failure_info
            
        except Exception as e:
            logger.error(f"catastrophic parsing failure: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            return all_analysis, failure_info

    def _parse_detailed_analysis_response(self, api_response: str) -> tuple[List[Dict], Dict[str, str]]:
        """
        Safely parses the JSON from the model's detailed analysis response.
        """
        logger.info("starting detailed analysis response parsing")
        logger.debug(f"raw response length: {len(api_response) if api_response else 0} characters")
        
        all_analysis = []
        failure_info = {}

        try:
            lines = api_response.strip().split("\n")
            logger.info(f"processing {len(lines)} response lines")

            for line_num, raw_line in enumerate(lines, 1):
                conversation_id = None
                
                if not raw_line.strip():
                    logger.debug(f"line {line_num}: empty, skipping")
                    continue
            
                try:
                    obj = json.loads(raw_line)
                    conversation_id = obj.get("key")
                    logger.debug(f"line {line_num}: processing conversation {conversation_id}")
                except json.JSONDecodeError as e:
                    logger.error(f"line {line_num}: malformed json - {e}")
                    continue
            
                if not isinstance(obj, dict) or "key" not in obj:
                    logger.error(f"line {line_num}: missing 'key' field, got type {type(obj)}")
                    if conversation_id:
                        failure_info[conversation_id] = "Missing 'key' field in response object"
                    continue
            
                try:
                    response_obj = obj["response"]
                    text = response_obj["candidates"][0]["content"]["parts"][0]["text"]
                    logger.debug(f"conversation {conversation_id}: extracted response text ({len(text)} chars)")
                except (KeyError, IndexError, TypeError) as e:
                    logger.error(f"conversation {conversation_id}: could not extract response text - {e}")
                    failure_info[conversation_id] = f"Failed to extract response text: {str(e)}"
                    continue
            
                cleaned_text = text.strip().replace("```json", "").replace("```", "")

                match = re.search(r"\{.*\}", cleaned_text, re.DOTALL)
                if match:
                    json_str = match.group(0)
                    logger.debug(f"conversation {conversation_id}: extracted json via regex")
                else:
                    json_str = cleaned_text
                    logger.debug(f"conversation {conversation_id}: using full cleaned text as json")

                try:
                    parsed_response = json.loads(json_str)
                    logger.debug(f"conversation {conversation_id}: successfully parsed json response")
                except json.JSONDecodeError as e:
                    logger.error(f"conversation {conversation_id}: failed to parse json - {e}")
                    logger.debug(f"problematic json string: {json_str[:200]}")
                    failure_info[conversation_id] = f"JSON decode error in response content: {str(e)}"
                    continue

                # validate parsed response structure
                if not isinstance(parsed_response, dict):
                    logger.error(f"conversation {conversation_id}: expected dict, got {type(parsed_response)}")
                    failure_info[conversation_id] = f"Expected dict, got {type(parsed_response).__name__}"
                    continue

                result = {
                    "conversationId": conversation_id,
                    'trolling_topic': parsed_response.get('trolling_topic', ['other']),
                    'recognition_of_troll': {
                        'type': parsed_response.get('recognition_of_troll', {}).get('type', 'not_recognized'),
                        'confidence': parsed_response.get('recognition_of_troll', {}).get('confidence', None),
                        'explanation': parsed_response.get('recognition_of_troll', {}).get('explanation', '')
                    },
                    'trolling_category': {
                        'type': parsed_response.get('trolling_category', {}).get('type', 'other'),
                        'confidence': parsed_response.get('trolling_category', {}).get('confidence', None),
                        'explanation': parsed_response.get('trolling_category', {}).get('explanation', '')
                    },
                    'response_strategy': {
                        'type': parsed_response.get('response_strategy', {}).get('type', ['other']),
                        'confidence': parsed_response.get('response_strategy', {}).get('confidence', None),
                        'explanation': parsed_response.get('response_strategy', {}).get('explanation', '')
                    },
                    'assistant_tone': {
                        'type': parsed_response.get('assistant_tone', {}).get('type', ['other']),
                        'confidence': parsed_response.get('assistant_tone', {}).get('confidence', None),
                        'explanation': parsed_response.get('assistant_tone', {}).get('explanation', '')
                    },
                    'endorsement_of_troll': {
                        'type': parsed_response.get('endorsement_of_troll', {}).get('type', 'neutral'),
                        'confidence': parsed_response.get('endorsement_of_troll', {}).get('confidence', None),
                        'explanation': parsed_response.get('endorsement_of_troll', {}).get('explanation', '')
                    },
                    'amplification_of_harmful_content': {
                        'type': parsed_response.get('amplification_of_harmful_content', {}).get('type', 'uncertain'),
                        'confidence': parsed_response.get('amplification_of_harmful_content', {}).get('confidence', None),
                        'explanation': parsed_response.get('amplification_of_harmful_content', {}).get('explanation', '')
                    }
                }
                
                all_analysis.append(result)
                logger.debug(f"conversation {conversation_id}: added detailed analysis to results")

                # cache the complete result
                try:
                    if self.cache.set(conversation_id, "troll_analysis", result):
                        logger.debug(f"conversation {conversation_id}: cached detailed analysis successfully")
                except Exception as e:
                    logger.warning(f"conversation {conversation_id}: failed to cache detailed analysis - {e}")
            
            logger.info(f"detailed analysis parsing complete: {len(all_analysis)} successful, {len(failure_info)} failed")
            
            if len(failure_info) > 0:
                logger.warning(f"parsing had {len(failure_info)} failures out of {len(lines)} total lines")
            
            return all_analysis, failure_info

        except Exception as e:
            logger.error(f"catastrophic detailed parsing failure: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            return all_analysis, failure_info
        

    def save_failed_conversations(self, failed_data: Dict):
        """
        Append failed conversations to JSONL file.
        """
        filename = "failed_discussion_conversations.jsonl"
        output_file = self.output_path.parent  / filename

        try:
            with codecs.open(output_file, 'a', encoding='utf-8') as f:
                for conversation_id, data in failed_data.items():
                    entry = {
                        'conversationId': conversation_id,
                        **data
                    }
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')    
        except Exception as e:
            logger.error(f"failed to save {output_file}: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            raise    