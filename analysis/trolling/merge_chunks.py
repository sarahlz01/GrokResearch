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

FIELDNAMES = [
    'conversationId',
    'is_trolling',
    'trolling_confidence',
    'trolling_intensity',
    'topic',
    'trolling_topic',
    'troll_recognition_type',
    'troll_recognition_confidence',
    'troll_recognition_explanation',
    'trolling_category_type',
    'trolling_category_confidence',
    'trolling_category_explanation',
    'response_strategy_type',
    'response_strategy_confidence',
    'response_strategy_explanation',
    'assistant_tone_type',
    'assistant_tone_confidence',
    'assistant_tone_explanation',
    'endorsement_type',
    'endorsement_confidence',
    'endorsement_explanation',
    'amplification_type',
    'amplification_confidence',
    'amplification_explanation'
]

OUTPUT_PATH = os.getenv("OUTPUT_PATH_TROLLING")

output_path = Path(OUTPUT_PATH)
base_path = output_path.parent
trolling_start_path = 'trolling_analysis_2025_'

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

def get_reply_count(folder_paths: List[Path], pattern: str = "output_raw_chunk_*.json") -> List[Dict]:
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
                    logger.info(f"DEBUG: Keys in {file.name}: {list(data.keys())}")
                    if 'analysis_results' in data:
                        results = data['analysis_results']                    
                        all_months.extend(results)
                        logger.info(f"{file.name} has {len(results)} conversations")
            except Exception as e:
                logger.info(f"Skipping due to {e}")
        
    logger.info(f"Found total of {len(all_months)} conversations")

    return all_months


def extract_replies(conversations: List[Dict]) -> List[Dict]:
    merged_data = []
    
    logger.info(f"DEBUG: Processing {len(conversations)} conversations")
    
    for conversation in conversations:
        conversationId = conversation.get('conversationId', '')

        if 'analysis' in conversation:
            analysis = conversation.get('analysis', {})
            is_trolling = analysis.get('is_trolling', '')
            
            if is_trolling != 'yes':
                continue
            
            intent_data = {
                'is_trolling': is_trolling,
                'trolling_confidence': analysis.get('trolling_confidence', ''),
                'trolling_intensity': analysis.get('trolling_intensity', ''),
                'topic': analysis.get('topic', ''),
                'trolling_topic': join_list_field(analysis.get('trolling_topic', []))
            }
            
            recognition = analysis.get('recognition_of_troll', {})
            category = analysis.get('trolling_category', {})
            strategy = analysis.get('response_strategy', {})
            tone = analysis.get('assistant_tone', {})
            endorsement = analysis.get('endorsement_of_troll', {})
            amplification = analysis.get('amplification_of_harmful_content', {})
            
        elif 'trolling_analysis' in conversation:
            trolling_analysis = conversation.get('trolling_analysis', {})
            intent = trolling_analysis.get('intent', {})
            detailed = trolling_analysis.get('detailed') or {}
            
            is_trolling = intent.get('is_trolling', '')
            
            if is_trolling != 'yes':
                continue
            
            intent_data = {
                'is_trolling': is_trolling,
                'trolling_confidence': intent.get('trolling_confidence', ''),
                'trolling_intensity': intent.get('trolling_intensity', ''),
                'topic': intent.get('topic', ''),
                'trolling_topic': join_list_field(detailed.get('trolling_topic', []))
            }
            
            recognition = detailed.get('recognition_of_troll', {})
            category = detailed.get('trolling_category', {})
            strategy = detailed.get('response_strategy', {})
            tone = detailed.get('assistant_tone', {})
            endorsement = detailed.get('endorsement_of_troll', {})
            amplification = detailed.get('amplification_of_harmful_content', {})
            
        else:
            logger.warning(f"Unknown structure: {conversation.keys()}")
            continue

        row = {
            'conversationId': conversationId,
            **intent_data,
            
            'troll_recognition_type': recognition.get('type', ''),
            'troll_recognition_confidence': recognition.get('confidence', ''),
            'troll_recognition_explanation': recognition.get('explanation', ''),
            
            'trolling_category_type': category.get('type', ''),
            'trolling_category_confidence': category.get('confidence', ''),
            'trolling_category_explanation': category.get('explanation', ''),
            
            'response_strategy_type': join_list_field(strategy.get('type', [])),
            'response_strategy_confidence': strategy.get('confidence', ''),
            'response_strategy_explanation': strategy.get('explanation', ''),
            
            'assistant_tone_type': join_list_field(tone.get('type', [])),
            'assistant_tone_confidence': tone.get('confidence', ''),
            'assistant_tone_explanation': tone.get('explanation', ''),
            
            'endorsement_type': endorsement.get('type', ''),
            'endorsement_confidence': endorsement.get('confidence', ''),
            'endorsement_explanation': endorsement.get('explanation', ''),
            
            'amplification_type': amplification.get('type', ''),
            'amplification_confidence': amplification.get('confidence', ''),
            'amplification_explanation': amplification.get('explanation', '')
        }
        
        merged_data.append(row)
    
    logger.info(f"Extracted {len(merged_data)} replies with {len(FIELDNAMES)} fields")
    return merged_data

def save_merged_data(merged_data: List[Dict], fieldnames: List[str]):
    filename = 'trolling_merged_results.csv'
    output_file = base_path / filename

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged_data)
        
        logger.info(f"Merged data saved to {output_file.resolve()}")
    except Exception as e:
        logger.error(f"Failed to save merged data to {output_file}: {e}")
        raise

def saved_merged_data_json(merged_data: List[Dict]):
    filename = 'trolling_merged_results.json'
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

    save_merged_data(merged_data=merged_data, fieldnames=FIELDNAMES)
    saved_merged_data_json(merged_data=merged_data)


if __name__ == "__main__":
    main()