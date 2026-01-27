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
        filename = f"report_chunk_{chunk_id}.json"
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

    def _parse_discussion_detection_response(self, api_response: str) -> List[Dict]:
        """
        Safely parses the JSON from the model's response.
        """
        logger.info("starting discussion detection response parsing")
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
            
                # parse the batch response line
                try:
                    obj = json.loads(raw_line)
                    conversation_id = obj.get("key")
                    logger.debug(f"line {line_num}: processing conversation {conversation_id}")
                except json.JSONDecodeError as e:
                    logger.error(f"line {line_num}: malformed json - {e}")
                    continue
            
                # validate obj structure
                if not isinstance(obj, dict) or "key" not in obj:
                    logger.error(f"line {line_num}: missing 'key' field, got type {type(obj)}")
                    if conversation_id:
                        failure_info[conversation_id] = f"Missing 'key' field in response object"
                    continue

                # extract response text
                try:
                    response_obj = obj["response"]
                    text = response_obj["candidates"][0]["content"]["parts"][0]["text"]
                    logger.debug(f"conversation {conversation_id}: extracted response text")
                except (KeyError, IndexError, TypeError) as e:
                    logger.error(f"conversation {conversation_id}: could not extract response text - {e}")
                    failure_info[conversation_id] = f"Failed to extract response text: {str(e)}"
                    continue
            
                # clean and parse JSON
                cleaned_text = text.strip().replace("```json", "").replace("```", "")
                
                match = re.search(r"\{.*\}", cleaned_text, re.DOTALL)
                if match:
                    json_str = match.group(0)
                    logger.info(f"conversation {conversation_id}: extracted json via regex")
                else:
                    json_str = cleaned_text
                    logger.info(f"conversation {conversation_id}: using full cleaned text as json")

                try:
                    parsed_response = json.loads(json_str)
                    logger.info(f"conversation {conversation_id}: successfully parsed json response")
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

                # extract and validate is_discussion field
                is_discussion = parsed_response.get('is_discussion', '').lower()
                if is_discussion not in ['yes', 'no', 'uncertain']:
                    logger.warning(
                        f"conversation {conversation_id}: invalid is_discussion value '{is_discussion}', treating as 'no'"
                    )
                    is_discussion = 'no'
                else:
                    logger.debug(f"conversation {conversation_id}: is_discussion = {is_discussion}")

                result = {
                    "conversationId": conversation_id,
                    'is_discussion': is_discussion,
                    'discussion_confidence': parsed_response.get('discussion_confidence'),
                    'discussion_intensity': parsed_response.get('discussion_intensity'),
                    'discussion_type': parsed_response.get('discussion_type'),
                    'topic': parsed_response.get('topic', '')
                }

                # validate numeric fields
                if result['discussion_confidence'] is not None:
                    try:
                        result['discussion_confidence'] = int(result['discussion_confidence'])
                        if not (0 <= result['discussion_confidence'] <= 5):
                            logger.warning(
                                f"conversation {conversation_id}: confidence {result['discussion_confidence']} "
                                f"out of range [0-5], setting to None"
                            )
                            result['discussion_confidence'] = None
                    except (ValueError, TypeError):
                        logger.warning(f"conversation {conversation_id}: invalid confidence value, setting to None")
                        result['discussion_confidence'] = None

                if result['discussion_intensity'] is not None:
                    try:
                        result['discussion_intensity'] = int(result['discussion_intensity'])
                        if not (0 <= result['discussion_intensity'] <= 5):
                            logger.warning(
                                f"conversation {conversation_id}: intensity {result['discussion_intensity']} "
                                f"out of range [0-5], setting to None"
                            )
                            result['discussion_intensity'] = None
                    except (ValueError, TypeError):
                        logger.warning(f"conversation {conversation_id}: invalid intensity value, setting to None")
                        result['discussion_intensity'] = None

                all_analysis.append(result)
                logger.debug(f"conversation {conversation_id}: added to results list")
                
                # cache the complete result
                try:
                    if self.cache.set(conversation_id, "discussion_detection", result):
                        logger.info(f"conversation {conversation_id}: cached successfully")
                except Exception as e:
                    logger.warning(f"conversation {conversation_id}: failed to cache - {e}")
            
            logger.info(f"discussion detection parsing complete: {len(all_analysis)} successful, {len(failure_info)} failed")
            
            if len(failure_info) > 0:
                logger.warning(f"parsing had {len(failure_info)} failures out of {len(lines)} total lines")
            
            return all_analysis, failure_info
            
        except Exception as e:
            logger.error(f"Catastrophic parsing failure: {e}")
            return all_analysis, failure_info

    def _parse_discussion_analysis_response(self, api_response: str) -> List[Dict]:
        """
        Safely parses the JSON from the model's detailed analysis response.
        """
        logger.info("starting discussion analysis response parsing")
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
                    logger.info(f"conversation {conversation_id}: extracted json via regex")
                else:
                    json_str = cleaned_text
                    logger.info(f"conversation {conversation_id}: using full cleaned text as json")

                try:
                    parsed_response = json.loads(json_str)
                    logger.info(f"conversation {conversation_id}: successfully parsed json response")
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
                    "discussion_type": parsed_response.get("discussion_type", ["other"]),
                    "bias_language": {
                        "bias_language": parsed_response.get("bias_language", {}).get("bias_language", "uncertain"),
                        "examples": parsed_response.get("bias_language", {}).get("examples", []),
                        "bias_confidence": parsed_response.get("bias_language", {}).get("bias_confidence", 1),
                        "assistant_bias": parsed_response.get("bias_language", {}).get("assistant_bias", ["none_detected"]),
                        "bias_intensity": parsed_response.get("bias_language", {}).get("bias_intensity", 0),
                    },
                    "assistant_stance": {
                        "stance": parsed_response.get("assistant_stance", {}).get("stance", "uncertain"),
                        "assistant_confidence": parsed_response.get("assistant_stance", {}).get("assistant_confidence", 1),
                        "assistant_bias": parsed_response.get("assistant_stance", {}).get("assistant_bias", ["none_detected"]),
                    },
                    "user_response": {
                        "type": parsed_response.get("user_response", {}).get("type", "other"),
                        "user_response_confidence": parsed_response.get("user_response", {}).get("user_response_confidence", 1),
                    }
                }
                
                all_analysis.append(result)
                logger.debug(f"conversation {conversation_id}: added detailed analysis to results")

                # cache the complete result
                try:
                    if self.cache.set(conversation_id, "discussion_analysis", result):
                        logger.info(f"conversation {conversation_id}: cached detailed analysis successfully")
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
        output_file = self.output_path.parent / filename

        try:
            with codecs.open(output_file, 'a', encoding='utf-8') as f:
                for conversation_id, data in failed_data.items():
                    entry = {
                        'conversationId': conversation_id,
                        **data
                    }
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                    logger.info(f"saved failed conversations to {output_file.resolve()}")
        except Exception as e:
            logger.error(f"failed to save {output_file}: {e}")
            logger.debug(f"traceback: {traceback.format_exc()}")
            raise