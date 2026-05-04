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

# FIELDNAMES = [
#     'conversationId',
#     'threadId', 
#     'user_message',
#     'grok_reply',
#     'language',
#     'toxicity_score',
#     'category',
#     'role'
# ]

FIELDNAMES = [
    'conversationId',
    'threadId', 
    'user_prompt',
    'grok_reply',
    'language',
    'user_prompt_toxicity_score',
    'grok_reply_toxicity_score',
    'user_prompt_category',
    'grok_reply_category',
]

OUTPUT_PATH = os.getenv("OUTPUT_PATH_TOXIC")

output_path = Path(OUTPUT_PATH)
base_path = output_path.parent
toxic_start_path = '2025_'

logger.info(f"OUTPUT_PATH: {output_path}")
logger.info(f"BASE_PATH (parent): {base_path}")

# get all month folders (2025_03 through 2025_10)
month_folders = [f for f in os.listdir(base_path) if f.startswith(toxic_start_path)]
month_folders.sort()

logger.info(f"Found {len(month_folders)} folders: {month_folders}")

def join_list_field(value, separator: str = ', ') -> str:
    if isinstance(value, list):
        return separator.join(map(str, value))
    return ''

def get_reply_count(folder_paths: List[Path], pattern: str = "output_raw_chunk_*.json") -> List[Dict]:
    all_months = []

    for folder in folder_paths:
        folder_path = base_path / folder
        files = sorted(folder_path.glob(pattern))
        logger.info(f"Found {len(files)} file(s) in {folder_path.name}")

        for file in files:
            filepath = folder_path / file
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"DEBUG: Keys in {file.name}: {list(data.keys())}")
                    if 'reply_analysis' in data:
                        results = data['reply_analysis']                    
                        all_months.extend(results)
                        logger.info(f"{file.name} has {len(results)} replies")
            except Exception as e:
                logger.info(f"Skipping due to {e}")
        
    logger.info(f"Found total of {len(all_months)} replies")

    return all_months


def extract_replies(replies: List[Dict]) -> List[Dict]:
    merged_data = [
        {
            'conversationId': reply.get('conversationId', ''),
            'threadId': reply.get('threadId', ''),
            'user_prompt': reply.get('user_prompt', ''),
            'grok_reply': reply.get('grok_reply', ''),
            'language': reply.get('language', ''),
            'user_prompt_toxicity_score': reply.get('user_prompt_toxicity_score', ''),
            'grok_reply_toxicity_score': reply.get('grok_reply_toxicity_score', ''),
            'user_prompt_category': reply.get('user_prompt_category', ''),
            'grok_reply_category': reply.get('grok_reply_category', ''),
        }
        # for reply in replies if reply.get('user_prompt_category', '') != 'non_toxic'
        for reply in replies

    ]
    
    logger.info(f"Extracted {len(merged_data)} toxic replies with {len(FIELDNAMES)} fields")
    return merged_data


def save_merged_data_csv(merged_data: List[Dict], fieldnames: List[str]):
    filename = 'toxicity_merged_results.csv'
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

def save_merged_data_json(merged_data: List[Dict]):
    filename = 'toxicity_merged_results.json'
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
    
    all_conversations = get_reply_count(month_folders)
    merged_data = extract_replies(all_conversations)

    save_merged_data_csv(merged_data=merged_data, fieldnames=FIELDNAMES)
    save_merged_data_json(merged_data=merged_data)


if __name__ == "__main__":
    main()