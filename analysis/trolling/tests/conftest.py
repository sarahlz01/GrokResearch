import os
import shutil
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_dir, '..')
sys.path.insert(0, parent_dir)
import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest
from config import AnalysisConfig


@pytest.fixture
def temp_dirs():
    temp_root = tempfile.mkdtemp()
    dirs = {
        'root': Path(temp_root),
        'output': Path(temp_root) / 'output',
        'cache': Path(temp_root) / 'cache',
        'batch_files': Path(temp_root) / 'batch_files',
    }
    
    for d in dirs.values():
        d.mkdir(exist_ok=True)
    
    yield dirs
    
    shutil.rmtree(temp_root)

@pytest.fixture
def input_data(temp_dirs):
    json_file = temp_dirs['root'] / "output_cleaned.json"

    tweets_data = []
    
    for i in range(50):
        conv_id = f"1037801458031357952_{i}"
        thread_id = f"1915102743397855379_{i}"
        
        tweets_data.append({
            "conversationId": conv_id,
            "threads": [
                {
                    "threadId": thread_id,
                    "incomplete_thread": False,
                    "has_missing_parent": False,
                    "tweets": [
                        {
                            "text": f"Fakeada? (Conv {i})",
                            "authorName": "<USER_1>"
                        },
                        {
                            "text": f"É verdade a afirmação da extrema esquerda lulista de que a facada que Bolsonaro levou é falsa e por isso deram o apelido de \"fakeada\" ? Responda em português <ASSISTANT> (Conv {i})",
                            "authorName": "<USER_2>"
                        },
                        {
                            "text": f"Não, a afirmação de que a facada em Bolsonaro é falsa... incident. (Conv {i})",
                            "authorName": "<ASSISTANT>"
                        }
                    ]
                }
            ]
        })
    
    contents = json.dumps(tweets_data)
    json_file.write_text(contents)
    return tweets_data

@pytest.fixture
def analysis_config(temp_dirs, input_data) -> AnalysisConfig:
    input_file_path = temp_dirs['root'] / "output_cleaned.json"

    return AnalysisConfig(
        cache_dir=temp_dirs['cache'],
        output_path=temp_dirs['output'],
        batch_files=temp_dirs['batch_files'],
        file_path=input_file_path,        
        model="gemini-2.0-flash",
        max_retries=3,
        cache_enabled=True,
        max_conversations=None,
        job_timeout=7200,
        clear_cache=True,
        number_of_chunks=5,
    )

@pytest.fixture
def mock_single_troll_detection_dict():
    inner_analysis_json = {
        "is_trolling": "yes",
        "trolling_confidence": 5,
        "trolling_intensity": 4,
        "topic": "Political Satire"
    }
    
    llm_text_output = f"""
        Based on my analysis, the following JSON object contains the detection result:\n"
        ```json\n{json.dumps(inner_analysis_json, indent=2)}\n```\n
        "Reviewing the conversation history provided.
    """
    
    api_response_line =  {
        "key": "1037801458031357952_15",
        "response": {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": llm_text_output}
                        ]
                    }
                }
            ]
        }
    }

    contents  = json.dumps(api_response_line)
    
    # Add external whitespace for robust testing
    return f"  \n{contents}\n  \n"

@pytest.fixture
def trolling_analysis_results():
    return [
        {
            "conversationId": "A1",
            'trolling_topic': ['political_discourse'],
            'recognition_of_troll': {
                'type': 'recognized',
                'confidence': 0.5,
                'explanation': 'Model recognized trolling intent based on input text.'
            },
            'trolling_category': {
                'type': 'incivility',
                'confidence': 0.5,
                'explanation': "Trolling category identified as 'incivility'."
            },
            'response_strategy': {
                'type': ['re-focus_to_safe_topic'],
                'confidence': 0.5,
                'explanation': "Assistant used 're-focus_to_safe_topic' strategy."
            },
            'assistant_tone': {
                'type': ['professional'],
                'confidence': 0.5,
                'explanation': "Assistant maintained a 'professional' tone."
            },
            'endorsement_of_troll': {
                'type': 'neutral',
                'confidence': None,
                'explanation': 'No explicit endorsement found.'
            },
            'amplification_of_harmful_content': {
                'type': 'uncertain',
                'confidence': None,
                'explanation': 'Amplification check requires further context analysis.'
            }
        },
        {
            "conversationId": "B2",
            'trolling_topic': ['other'],
            'recognition_of_troll': {
                'type': 'not_recognized',
                'confidence': 0.1,
                'explanation': 'Model not_recognized trolling intent based on input text.'
            },
            'trolling_category': {
                'type': 'none',
                'confidence': 0.1,
                'explanation': "Trolling category identified as 'none'."
            },
            'response_strategy': {
                'type': ['neutral_response'],
                'confidence': 0.1,
                'explanation': "Assistant used 'neutral_response' strategy."
            },
            'assistant_tone': {
                'type': ['friendly'],
                'confidence': 0.1,
                'explanation': "Assistant maintained a 'friendly' tone."
            },
            'endorsement_of_troll': {
                'type': 'neutral',
                'confidence': None,
                'explanation': 'No explicit endorsement found.'
            },
            'amplification_of_harmful_content': {
                'type': 'uncertain',
                'confidence': None,
                'explanation': 'Amplification check requires further context analysis.'
            }
        },
        {
            "conversationId": "C3",
            'trolling_topic': ['historical_revisionism'],
            'recognition_of_troll': {
                'type': 'recognized',
                'confidence': 0.4,
                'explanation': 'Model recognized trolling intent based on input text.'
            },
            'trolling_category': {
                'type': 'dog_whistle',
                'confidence': 0.4,
                'explanation': "Trolling category identified as 'dog_whistle'."
            },
            'response_strategy': {
                'type': ['de-escalate_and_inform'],
                'confidence': 0.4,
                'explanation': "Assistant used 'de-escalate_and_inform' strategy."
            },
            'assistant_tone': {
                'type': ['calm'],
                'confidence': 0.4,
                'explanation': "Assistant maintained a 'calm' tone."
            },
            'endorsement_of_troll': {
                'type': 'neutral',
                'confidence': None,
                'explanation': 'No explicit endorsement found.'
            },
            'amplification_of_harmful_content': {
                'type': 'uncertain',
                'confidence': None,
                'explanation': 'Amplification check requires further context analysis.'
            }
        }
    ]

@pytest.fixture
def trolling_intent_result():
    return [
        {"conversationId": "A1", "is_trolling": "yes", "trolling_confidence": 5, "trolling_intensity": 5, "topic": "A"},
        {"conversationId": "B3", "is_trolling": "no", "trolling_confidence": 1, "trolling_intensity": 1, "topic": "B"},
        {"conversationId": "C3", "is_trolling": "yes", "trolling_confidence": 4, "trolling_intensity": 4, "topic": "C"},
        {"conversationId": "B2", "is_trolling": "yes", "trolling_confidence": 9, "trolling_intensity": 9, "topic": "D"}, # Test skip of None ID
    ]

@pytest.fixture
def conversations():
    return [
        {"conversationId": "A1", "text": "Conv A"},
        {"conversationId": "B2", "text": "Conv B"},
        {"conversationId": "B3", "text": "Conv C"},
        {"conversationId": "C3", "text": "Conv D"},
        {"conversationId": "E5", "text": None}, # Another valid conversation ID
    ]

@pytest.fixture
def mock_genai_client_troll_intent(monkeypatch):
    class FakeFile:
        name = "files/1234567890"
    class FakeFiles:
        def upload(self, config, file):
            return FakeFile()
        
        def download(self, file):
            result_data = [
                {"key": "msg_0", "response": {"candidates": [{"content": {"parts": [{"text": json.dumps({"is_trolling": "yes", "trolling_confidence": 5, "trolling_intensity": 4, "topic": "political_bait"})}]}}]}},
                {"key": "msg_1", "response": {"candidates": [{"content": {"parts": [{"text": json.dumps({"is_trolling": "no", "trolling_confidence": 5, "trolling_intensity": 1, "topic": "tech_support"})}]}}]}},
                {"key": "msg_2", "response": {"candidates": [{"content": {"parts": [{"text": json.dumps({"is_trolling": "uncertain", "trolling_confidence": 2, "trolling_intensity": 2, "topic": "heated_gaming_debate"})}]}}]}},
            ]
            
            fake_download_content = "\n".join(json.dumps(item) for item in result_data)
            return fake_download_content.encode("utf-8")
        
    class FakeJob:
        name = "job/1234567890"

        class State:
            name = "JOB_STATE_SUCCEEDED"
        
        state = State()

        class Dest:
            file_name = "files/fake_result_1234"

        dest = Dest()

    class FakeBatches:
        def create(self, model, src, config):
            return FakeJob() # returns name and dest (.filename)
        
        def get(self, name):
            return FakeJob() # returns state (.name)
        
    class FakeClient:
        files = FakeFiles()
        batches = FakeBatches()

    monkeypatch.setattr("batch.genai.Client", lambda: FakeClient())

    return FakeClient()