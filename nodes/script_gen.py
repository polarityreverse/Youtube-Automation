import logging
import json
import re
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Dict, Any

from utils.schema import flowstate
from utils.sheets import get_worksheet
from config import (
    CLAUDE_API_KEY, SCRIPT_GENERATION_PROMPT, CLAUDE_MODEL, 
    CLAUDE_SCRIPT_IMAGE_PROMPT_URL, SCRIPT_GENERATION_SYSTEM_INSTRUCTIONS
)

# Set up structured logging for AWS CloudWatch
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def call_claude_api(payload: Dict, headers: Dict) -> Dict:
    """Wrapper with retry logic for AWS stability."""
    response = requests.post(
        CLAUDE_SCRIPT_IMAGE_PROMPT_URL, 
        headers=headers, 
        json=payload, 
        timeout=60
    )
    response.raise_for_status()
    return response.json()

def script_generation(state: flowstate) -> flowstate:
    """Node 1: Script generation with production-grade logging and validation."""
    
    row_idx = state.get('row_index')
    picked_idea = state.get('idea')
    
    # Contextual logging for CloudWatch tracking
    log_extra = {"row_index": row_idx, "topic": picked_idea}
    logger.info(f"Starting script generation for: {picked_idea}", extra=log_extra)

    try:
        worksheet = get_worksheet("ideas")
        
        # 1. CACHE CHECK (Optimized: Get row once if possible, rather than cell by cell)
        # Production Tip: If scaling, move this cache from Sheets to DynamoDB or Redis
        cached_script_raw = worksheet.cell(row_idx, 3).value
        
        if cached_script_raw and cached_script_raw.strip():
            try:
                state["script"] = json.loads(cached_script_raw)
                state["isscriptgenerated"] = True
                logger.info(f"Cache Hit: Loaded script from Sheet row {row_idx}")
                return state
            except json.JSONDecodeError:
                logger.warning(f"Cache Corrupt: Row {row_idx} contained invalid JSON.")

        # 2. API PREPARATION
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 4000,
            "system": SCRIPT_GENERATION_SYSTEM_INSTRUCTIONS,
            "messages": [{"role": "user", "content": f"{SCRIPT_GENERATION_PROMPT} {picked_idea}"}]
        }
        headers = {
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }

        # 3. API CALL WITH RETRIES
        response_json = call_claude_api(payload, headers)
        script_text = response_json["content"][0]["text"]

        # 4. ROBUST EXTRACTION
        json_match = re.search(r'(\{.*\}|\[.*\])', script_text, re.DOTALL)
        if not json_match:
            logger.error(f"Format Error: No JSON block in Claude response for row {row_idx}")
            raise ValueError("Claude failed to return a JSON block.")

        generated_script = json.loads(json_match.group(1))
        
        # 5. SCHEMA VALIDATION (Crucial for Node 2/3 stability)
        required_keys = ["Metadata", "scenes"]
        if not all(key in generated_script for key in required_keys):
            raise KeyError(f"Invalid Script Schema: Missing keys {required_keys}")

        # 6. STATE UPDATE
        state["script"] = generated_script
        metadata = generated_script.get("Metadata", {})
        
        # Use .get() with defaults everywhere to prevent crashes
        state["topic_comment"] = metadata.get("Topic_Comment", "COMMENT 'SCIENCE' FOR MORE!")
        state["isscriptgenerated"] = True
        
        # 7. PERSISTENCE
        # Use batch updates if updating multiple cells to save API quota
        worksheet.update_cell(row_idx, 3, json.dumps(generated_script))
        logger.info(f"Success: Script generated and persisted for Row {row_idx}")

    except Exception as e:
        # Structured error logging
        logger.error(f"Node 1 Failure: {str(e)}", exc_info=True)
        state["isscriptgenerated"] = False
        # In production, you might want to send an alert to AWS SNS here
        
    return state