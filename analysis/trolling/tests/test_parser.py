import json
from typing import Dict, List

import pytest
from cache import AnalysisCache
from storage import EncodingHandler


def test_troll_detection_parser(analysis_config, mock_single_troll_detection_dict):
    cache = AnalysisCache(analysis_config)
    storage = EncodingHandler(analysis_config, cache)

    intent = storage._parse_troll_detection_response(mock_single_troll_detection_dict)

    assert isinstance(intent, List)
    
    assert len(intent) == 1
    
    first_intent = intent[0]
    
    assert isinstance(first_intent, Dict)
    assert "is_trolling" in first_intent
    assert first_intent["is_trolling"] == "yes"
    
    assert "trolling_confidence" in first_intent
    assert first_intent["trolling_confidence"] == 5
    
    assert first_intent["topic"] == "Political Satire"

    assert "conversationId" in first_intent

    
def test_troll_intent_preprocessing_logic(analysis_config, trolling_intent_result, trolling_analysis_results, conversations):
    chunk_id = 1
    cache =  AnalysisCache(analysis_config)

    final_results = []
    intent_map = {}
    trolling_conversation_ids = set()  # only "yes" responses for detailed analysis

    for result in trolling_intent_result:
        conv_id = result.get("conversationId")
        is_trolling = result.get("is_trolling", "").lower()
        
        # store all results
        intent_map[conv_id] = result
        
        # track only trolling conversations for detailed analysis
        if is_trolling == "yes":
            trolling_conversation_ids.add(conv_id)

    # build conversation lookup for ALL conversations
    conversation_map = {
        conv.get('conversationId'): conv
        for conv in conversations
        if conv.get('conversationId')
    }
        
    if trolling_conversation_ids:
        # filter to only trolling conversations for detailed analysis
        trolling_conversations = [
            conv for conv in conversations 
            if conv.get('conversationId') in trolling_conversation_ids
        ]
        
    assert len(intent_map) == 4
    assert "A1" in intent_map
    assert "B2" in intent_map
    assert "C3" in intent_map
    assert intent_map["A1"]["is_trolling"] == "yes"

    assert len(trolling_conversation_ids) == 3
    # assert trolling_conversation_ids == {"A1", "C3"}
    assert "B2" in trolling_conversation_ids

    assert len(conversation_map) == 5
    assert "C3" in conversation_map 
    assert "E5" in conversation_map

    assert len(trolling_conversations) == 3
    conv_ids_filtered = {c['conversationId'] for c in trolling_conversations}
    assert conv_ids_filtered == {"A1", "C3", "B2"}
            
    if trolling_conversation_ids:
        # filter to only trolling conversations for detailed analysis
        trolling_conversations = [
            conv for conv in conversations 
            if conv.get('conversationId') in trolling_conversation_ids
        ]

    
        if trolling_analysis_results:
            detailed_map = {
                result.get('conversationId'): result 
                for result in trolling_analysis_results
                if result.get('conversationId')
            }

    for conv_id, intent_result in intent_map.items():
        conversation = conversation_map.get(conv_id)
        is_trolling = intent_result.get("is_trolling", "").lower()
        
        combined = {
            'conversationId': conv_id,
            'original_conversation': conversation,
            'trolling_analysis': {
                'intent': intent_result.copy()
            }
        }
        
        if is_trolling == "yes":
            if conv_id in detailed_map:
                combined['trolling_analysis']['detailed'] = detailed_map[conv_id]
            else:
                combined['trolling_analysis']['detailed'] = None
        else:
            combined['trolling_analysis']['detailed'] = None
        
        final_results.append(combined)

    assert sorted([c['conversationId'] for c in trolling_conversations]) == ['A1', 'B2', 'C3']

    assert len(detailed_map) == 3, "Detailed map should only contain A1 and C3 (from MOCK_DETAILED_ANALYSIS)."
    assert 'A1' in detailed_map and 'C3' in detailed_map, "A1 and C3 should be keys in detailed_map."

    assert len(final_results) == 4, "Final results should include A1, B2, C3, and E6."
    
    result_a1 = next(r for r in final_results if r['conversationId'] == 'A1')
    
    assert result_a1['original_conversation']['text'] == 'Conv A'
    assert result_a1['trolling_analysis']['intent']['is_trolling'].lower() == 'yes'
    assert result_a1['trolling_analysis']['detailed'] is not None

    result_b2 = next(r for r in final_results if r['conversationId'] == 'B2')
    
    assert result_b2['trolling_analysis']['intent']['is_trolling'].lower() == 'yes'

    output = {
        "analysis_config": final_results
    }

    filename = f"report_chunk_{chunk_id}.json"

    storage = EncodingHandler(analysis_config, cache)
    storage.save_json_with_encoding(output, chunk_id)

    file_path =  storage.output_path / filename 

    assert file_path.exists()

    with open(file_path, 'r', encoding='utf-8') as file:
        contents = file.read()

    assert contents

    load_contents  = json.loads(contents)

    assert load_contents == output
    
    assert load_contents["analysis_config"]

    first = load_contents["analysis_config"][0]

    assert "trolling_analysis" in first

    assert "detailed" in first["trolling_analysis"]

    second = first["trolling_analysis"]["detailed"]

    assert second is not None
