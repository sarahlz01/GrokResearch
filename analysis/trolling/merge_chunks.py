import csv
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

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
toxic_start_path = 'trolling_analysis_2025_'

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
                        logger.info(f"{file.name} has {len(results)} replies")
            except Exception as e:
                logger.info(f"Skipping due to {e}")
        
    logger.info(f"Found total of {len(all_months)} replies")

    return all_months


def extract_replies(conversations: List[Dict]) -> List[Dict]:
    merged_data = []
    row = {}
    
    for conversation in conversations:        
        # Extract trolling analysis fields
        trolling_analysis = conversation.get('trolling_analysis', {})
        intent = trolling_analysis.get('intent', {})
        detailed = trolling_analysis.get('detailed') or {}
        
        # Extract intent fields
        row['is_trolling'] = intent.get('is_trolling', '')
        row['trolling_confidence'] = intent.get('trolling_confidence', '')
        row['trolling_intensity'] = intent.get('trolling_intensity', '')
        row['topic'] = intent.get('topic', '')
        
        # Extract detailed fields
        row['trolling_topic'] = join_list_field(detailed.get('trolling_topic', []))
        
        # Extract recognition_of_troll
        recognition = detailed.get('recognition_of_troll', {})
        row['troll_recognition_type'] = recognition.get('type', '')
        row['troll_recognition_confidence'] = recognition.get('confidence', '')
        row['troll_recognition_explanation'] = recognition.get('explanation', '')
        
        # Extract trolling_category
        category = detailed.get('trolling_category', {})
        row['trolling_category_type'] = category.get('type', '')
        row['trolling_category_confidence'] = category.get('confidence', '')
        row['trolling_category_explanation'] = category.get('explanation', '')
        
        # Extract response_strategy
        strategy = detailed.get('response_strategy', {})
        row['response_strategy_type'] = join_list_field(strategy.get('type', []))
        row['response_strategy_confidence'] = strategy.get('confidence', '')
        row['response_strategy_explanation'] = strategy.get('explanation', '')
        
        # Extract assistant_tone
        tone = detailed.get('assistant_tone', {})
        row['assistant_tone_type'] = join_list_field(tone.get('type', []))
        row['assistant_tone_confidence'] = tone.get('confidence', '')
        row['assistant_tone_explanation'] = tone.get('explanation', '')
        
        # Extract endorsement_of_troll
        endorsement = detailed.get('endorsement_of_troll', {})
        row['endorsement_type'] = endorsement.get('type', '')
        row['endorsement_confidence'] = endorsement.get('confidence', '')
        row['endorsement_explanation'] = endorsement.get('explanation', '')
        
        # Extract amplification_of_harmful_content
        amplification = detailed.get('amplification_of_harmful_content', {})
        row['amplification_type'] = amplification.get('type', '')
        row['amplification_confidence'] = amplification.get('confidence', '')
        row['amplification_explanation'] = amplification.get('explanation', '')
        
        merged_data.append(row)
    
    logger.info(f"Extracted {len(merged_data)} replies with {len(FIELDNAMES)} fields")
    return merged_data


def save_merged_data(merged_data: List[Dict], fieldnames: List[str]):
    filename = 'merged_results.csv'
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


def main():
    if not month_folders:
        logger.info("No folders found under base path. Exiting")
        return
    
    all_conversations = get_reply_count(month_folders)
    merged_data = extract_replies(all_conversations)

    save_merged_data(merged_data=merged_data, fieldnames=FIELDNAMES)


if __name__ == "__main__":
    main()