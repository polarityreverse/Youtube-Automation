import os
import base64
import requests
import json
import random
import time
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import ELEVENLABS_API_KEY, ELEVENLABS_VOICE_GENERATION_API_URL, OUTPUT_DIR, VOICE_IDS

# Configure logger for AWS CloudWatch
logger = logging.getLogger(__name__)
# basicConfig is assumed to be configured in main.py, 
# but we use logger.info/error for consistency.

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    reraise=True
)
def call_elevenlabs_api(url: str, payload: dict, headers: dict):
    """Retries on 429 (Rate Limit) or 5xx errors automatically."""
    response = requests.post(url, json=payload, headers=headers, timeout=90)
    
    if response.status_code == 429:
        logger.warning("🕒 ElevenLabs Rate Limit hit. Tenacity will backoff and retry...")
    
    response.raise_for_status()
    return response.json()

def audio_generation(state: dict) -> dict:
    """Node 2: Optimized for Word-Level Alignment and Seamless Looping with Production-grade Logging."""

    if not state.get("isscriptgenerated"):
        logger.warning("⚠️ Skipping Audio: Script was not generated in previous node.")
        return state

    row_id = state.get('row_index')
    script_data = state.get('script')
    
    # Contextual logging for easy AWS debugging
    log_extra = {"row_index": row_id}
    logger.info(f"Starting Audio Generation for Row {row_id}", extra=log_extra)
    
    # Define File Paths
    final_vo_path = os.path.join(OUTPUT_DIR, f"vo_row_{row_id}.mp3")
    alignment_filename = os.path.join(OUTPUT_DIR, f"alignment_row_{row_id}.json")

    # --- 1. CACHE CHECK ---
    if os.path.exists(final_vo_path) and os.path.exists(alignment_filename):
        logger.info(f"📦 Cache Hit: Loading audio and alignment for Row {row_id}...")
        try:
            with open(alignment_filename, 'r') as f:
                state["alignment_data"] = json.load(f)
            state["vo_path"] = final_vo_path
            state["isvoicegenerated"] = True
            return state
        except Exception as e:
            logger.error(f"⚠️ Cache Corrupted for Row {row_id}, regenerating: {e}")

    # --- 2. PREPARE TEXT FOR SEAMLESS LOOP ---
    try:
        scenes = script_data['scenes']
        full_vo_text = " ".join([scene['Voiceover_English'].strip() for scene in scenes])

        # --- 3. API CONFIGURATION ---
        headers = {
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json"
        }
        
        VOICE_ID = random.choice(VOICE_IDS)
        
        vo_payload = {
            "text": full_vo_text,
            "model_id": "eleven_multilingual_v2",
            "voice_settings": {
                "stability": 0.45,       # Pacing optimization
                "similarity_boost": 0.8,
                "style": 0.0,            # Preventing dramatic pauses
                "use_speaker_boost": True
            }
        }

        # --- 4. API CALL WITH TIMESTAMPS ---
        # Using the with-timestamps endpoint for word-level captioning
        vo_url = f"{ELEVENLABS_VOICE_GENERATION_API_URL}/{VOICE_ID}/with-timestamps"
        
        # Execute API call with built-in retry logic
        data = call_elevenlabs_api(vo_url, vo_payload, headers)
        
        audio_bytes = base64.b64decode(data['audio_base64'])
        
        # Save Audio
        with open(final_vo_path, 'wb') as f:
            f.write(audio_bytes)

        # Save Alignment
        with open(alignment_filename, 'w') as f:
            json.dump(data['alignment'], f)
        
        # --- 5. UPDATE STATE FOR ASSEMBLY ---
        state["vo_path"] = final_vo_path
        state["alignment_data"] = data['alignment']
        state["isvoicegenerated"] = True
        
        logger.info(f"✅ VO & Alignment successfully saved for Row {row_id}")

    except Exception as e:
        logger.error(f"❌ VO Generation Failed for Row {row_id}: {str(e)}", exc_info=True)
        state["isvoicegenerated"] = False
    
    return state