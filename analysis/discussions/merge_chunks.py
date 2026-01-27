import codecs
import csv
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
OUTPUT_PATH = os.getenv("OUTPUT_PATH_DISCUSSION")

FIELDNAMES = [
    'conversationId',
    'is_discussion',
    'discussion_confidence',
    'discussion_intensity',
    'discussion_type',
    'topic',
    'bias_language',
    'bias_examples',
    'bias_confidence',
    'assistant_bias',
    'bias_intensity',
    'assistant_stance',
    'stance_confidence',
    'assistant_stance_bias',
    'user_response_type',
    'user_response_confidence'
]

output_path = Path(OUTPUT_PATH)
base_path = output_path.parent
trolling_start_path = 'discussion_analysis_2025_'

logger.info(f"OUTPUT_PATH: {output_path}")
logger.info(f"BASE_PATH (parent): {base_path}")

# get all month folders (2025_03 through 2025_10)
month_folders = [f for f in os.listdir(base_path) if f.startswith(trolling_start_path)]
month_folders.sort()

logger.info(f"Found {len(month_folders)} folders: {month_folders}")

def join_list_field(value, separator: str = ', ') -> str:
    if isinstance(value, list):
        return separator.join(map(str, value))
    return ''

def get_conversation_counts(folder_paths: List[Path], pattern: str = "report_chunk_*.json") -> List[Dict]:
    all_months = []

    for folder in folder_paths:
        folder_path = base_path / folder
        files = sorted(folder_path.glob(pattern))
        logger.info(f"Found {len(files)} files in {folder_path.name}")

        for file in files:
            filepath = folder_path / file
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    if 'analysis_results' in data:
                        results = data['analysis_results']
                        if len(results) > 0:
                            for result in results:
                                result['source_folder'] = folder_path
                                result['source_file'] = file
                                
                            all_months.extend(results)
                            logger.info(f"{file.name} has {len(results)} conversations")
            except Exception as e:
                logger.info(f"Skipping due to {e}")
        
    logger.info(f"Found total of {len(all_months)} conversations")

    return all_months


def merge_chunks(conversations: List[Dict]):
    merged_data = []

    for conversation in conversations:
        conversation_id = conversation.get("conversationId")

        discussion_analysis = conversation.get('discussion_analysis', {})
        intent = discussion_analysis.get('intent', {})
        is_discussion = intent.get('is_discussion', '')
        
        if is_discussion != 'yes':
            continue

        detailed = discussion_analysis.get('detailed') or {}
        bias_lang = detailed.get('bias_language', {})
        assist_stance = detailed.get('assistant_stance', {})
        user_resp = detailed.get('user_response', {})


        intent_data = {
            # intent values
            'is_discussion': intent.get('is_discussion', ''),
            'discussion_confidence': intent.get('discussion_confidence', ''),
            'discussion_intensity': intent.get('discussion_intensity', ''),
            'discussion_type': join_list_field(intent.get('discussion_type')),
            'topic': intent.get('topic', ''),
        }
        
        # build row
        row = {
            'conversationId': conversation_id,
            **intent_data,
            
            # bias values
            'bias_language': bias_lang.get('bias_language', ''),
            'bias_examples': join_list_field(bias_lang.get('examples')),
            'bias_confidence': bias_lang.get('bias_confidence', ''),
            'assistant_bias': join_list_field(bias_lang.get('assistant_bias')),
            'bias_intensity': bias_lang.get('bias_intensity', ''),

            # assistance stance
            'assistant_stance': assist_stance.get('stance', ''),
            'stance_confidence': assist_stance.get('assistant_confidence', ''),
            'assistant_stance_bias': join_list_field(assist_stance.get('assistant_bias')),

            # user response
            'user_response_type': user_resp.get('type', ''),
            'user_response_confidence': user_resp.get('user_response_confidence', '')
        }

        merged_data.append(row)
        
    logger.info(f"Merged results contain {len(merged_data)} conversations")

    return merged_data


def save_merged_data_csv(merged_data: List[Dict], fieldnames: List[str]):
    filename = 'discussions_merged_results.csv'
    output_file = base_path / filename

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(merged_data)
        logger.info(f"Merged data saved to {output_file.resolve()}")
    except Exception as e:
        logger.error(f"Failed to save merged data to {output_file}: {e}")
        raise

def saved_merged_data_json(merged_data: List[Dict], fieldnames: List[str]):
    filename = 'discussions_merged_results.json'
    output_file = base_path / filename

    try:
        with codecs.open(output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Merged data saved to {output_file.resolve()}")
    except Exception as e:
        logger.error(f"failed to save {output_file}: {e}")
        raise


def main():
    if not month_folders:
        logger.info("No folders found under base path. Exiting")
        return
    
    all_conversations = get_conversation_counts(month_folders)
    merged_conversations = merge_chunks(all_conversations)

    save_merged_data_csv(merged_conversations, FIELDNAMES)
    saved_merged_data_json(merged_conversations, FIELDNAMES)


if __name__ == "__main__":
    main()