import os
import json
import time
import requests
import logging
from googleapiclient.http import MediaFileUpload
from google import genai
from google.genai import types

from utils.schema import flowstate
from utils.youtube_auth import get_youtube_client
from utils.sheets import get_worksheet
from config import (
    OUTPUT_DIR, INSTA_ACCESS_TOKEN, INSTA_ACCOUNT_ID, 
    GEMINI_API_KEY_1, VIDEO_METADATA_GENERATION_MODEL
)

# Set up production logging
logger = logging.getLogger(__name__)

# --- HELPER 1: Metadata Generator ---
def get_llm_metadata(topic):
    """Generates viral metadata with CTA, Pausing, and User Engagement focus."""
    client = genai.Client(api_key=GEMINI_API_KEY_1)
    
    prompt = f"""
Act as a Senior YouTube Strategist for 'Zeteon'.
Topic: '{topic}'

YOUR TASK:
1. Create a high-retention title including #Shorts.
2. The description must include a clear Call to Action (CTA).
3. The pinned_comment must be a 'pausing' prompt—a surprising fact or question that encourages a reply.
4. Instagram caption must be punchy and optimized for Reels.

STRICT OUTPUT FORMAT:
Return ONLY a JSON object. No markdown, no explanations.

REQUIRED JSON STRUCTURE:
{{
    "youtube": {{
        "title": "string",
        "description": "string",
        "tags": ["#tag1", "#tag2"],
        "pinned_comment": "string"
    }},
    "insta": {{
        "caption": "string",
        "hashtags": ["#tag1", "#tag2"]
    }}
}}
"""

    try:
        response = client.models.generate_content(
            model=VIDEO_METADATA_GENERATION_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.2,
            )
        )
        
        data = json.loads(response.text.strip())
        if isinstance(data, list): data = data[0]

        if 'youtube' in data and 'insta' in data:
            logger.info(f"✅ Metadata successfully generated for: {topic}")
            return data
        else:
            logger.error(f"❌ LLM Schema Drift: {list(data.keys())}")
            return None

    except Exception as e:
        logger.error(f"❌ Metadata Gen Error: {str(e)}")
        return None

# --- HELPER 2: YouTube Uploader ---
def upload_to_youtube(video_path, metadata, row_idx):
    """Uploads to YouTube and handles engagement pinning."""
    try:
        youtube = get_youtube_client()
        
        body = {
            'snippet': {
                'title': metadata['title'][:100],
                'description': metadata['description'],
                'tags': metadata.get('tags', []),
                'categoryId': '28' 
            },
            'status': {'privacyStatus': 'public', 'selfDeclaredMadeForKids': False}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = request.execute()
        
        video_id = response['id']
        video_url = f"https://www.youtube.com/shorts/{video_id}"
        logger.info(f"🎬 YouTube Uploaded: {video_url}")

        # Wait for Processing
        for _ in range(15): 
            time.sleep(45)
            status_res = youtube.videos().list(part="processingDetails", id=video_id).execute()
            items = status_res.get("items", [])
            if not items: break
            if items[0].get("processingDetails", {}).get("processingStatus") == "succeeded":
                logger.info("✅ YouTube: Processing complete.")
                break
        
        # Add Pinned Engagement Comment
        youtube.commentThreads().insert(
            part="snippet",
            body={
                "snippet": {
                    "videoId": video_id,
                    "topLevelComment": {"snippet": {"textOriginal": metadata['pinned_comment']}}
                }
            }
        ).execute()
        
        return "SUCCESS", video_url
    except Exception as e:
        logger.error(f"❌ YouTube Error: {e}")
        return "FAILED", None

# --- HELPER 3: Instagram Uploader (FINAL ROBUST VERSION) ---
def upload_to_insta(video_url, metadata):
    """Uploads to Instagram with a staged polling to handle ShadowIGMediaBuilder errors."""
    base_url = f"https://graph.facebook.com/v19.0/{INSTA_ACCOUNT_ID}"
    caption = f"{metadata['caption']}\n\n{' '.join(metadata['hashtags'])}"
    
    try:
        # Step 1: Create Container
        res = requests.post(f"{base_url}/media", data={
            'video_url': video_url, 
            'caption': caption,
            'media_type': 'REELS', 
            'access_token': INSTA_ACCESS_TOKEN
        }, timeout=60)
        
        container_data = res.json()
        container_id = container_data.get('id')
        
        if not container_id:
            logger.error(f"❌ Container ID missing: {container_data}")
            return "FAILED"

        # Step 2: Staged Polling
        for i in range(45):
            time.sleep(20)
            
            # Request ONLY status_code first to avoid ShadowIGMediaBuilder field errors
            status_res = requests.get(
                f"https://graph.facebook.com/v19.0/{container_id}", 
                params={'fields': 'status_code', 'access_token': INSTA_ACCESS_TOKEN}
            ).json()
            
            s_code = status_res.get('status_code')
            
            if s_code is None:
                logger.info(f"⏳ Attempt {i+1}/45: Container initializing...")
                continue
                
            logger.info(f"⏳ Attempt {i+1}/45: Meta Status is '{s_code}'") 

            if s_code == 'FINISHED':
                publish_res = requests.post(
                    f"{base_url}/media_publish", 
                    data={'creation_id': container_id, 'access_token': INSTA_ACCESS_TOKEN}
                ).json()
                
                if "id" in publish_res:
                    logger.info("✅ Instagram Reel Published Successfully!")
                    return "SUCCESS"
                else:
                    logger.error(f"❌ Publish failed: {publish_res}")
                    return "FAILED"
            
            elif s_code == 'ERROR':
                # Only ask for error_message if we know an error exists
                err_data = requests.get(
                    f"https://graph.facebook.com/v19.0/{container_id}", 
                    params={'fields': 'error_message', 'access_token': INSTA_ACCESS_TOKEN}
                ).json()
                logger.error(f"❌ Meta Error: {err_data.get('error_message')}")
                return "FAILED"

        logger.error("🚨 Instagram Processing Timed Out after 15 minutes.")
        return "FAILED"
    except Exception as e:
        logger.error(f"⚠️ Insta Exception: {e}")
        return "ERROR"

# --- MAIN EXECUTION NODE ---
def video_upload_node(state: flowstate) -> flowstate:
    row_idx = state['row_index']
    topic = state['idea']
    
    logger.info(f"🚀 Starting Zeteon Final Sync (Row {row_idx})")

    try:
        worksheet = get_worksheet("ideas")
        final_video_path = os.path.join(OUTPUT_DIR, f"Video_Row_{row_idx}.mp4")
        github_video_uri = worksheet.cell(row_idx, 4).value 

        youtube_status = worksheet.cell(row_idx, 5).value
        insta_status = worksheet.cell(row_idx, 7).value

        # Default to False
        state["isvideouploaded"] = False

        if youtube_status != "UPLOADED" or insta_status != "UPLOADED":
            meta = get_llm_metadata(topic)
            if not meta: return state

            # 1. YouTube Step
            if youtube_status != "UPLOADED":
                status, yt_link = upload_to_youtube(final_video_path, meta['youtube'], row_idx)
                if status == "SUCCESS":
                    worksheet.update_cell(row_idx, 5, "UPLOADED")
                    worksheet.update_cell(row_idx, 6, json.dumps(meta['youtube']))
                    worksheet.update_cell(row_idx, 9, yt_link)
                    youtube_status = worksheet.cell(row_idx, 5).value

                else:
                    return state

            # 2. Instagram Step
            if insta_status != "UPLOADED":
                if upload_to_insta(github_video_uri, meta['insta']) == "SUCCESS":
                    worksheet.update_cell(row_idx, 7, "UPLOADED")
                    worksheet.update_cell(row_idx, 8, json.dumps(meta['insta']))
                    insta_status = worksheet.cell(row_idx, 7).value
                else:
                    return state

        # Verification
        if worksheet.cell(row_idx, 5).value == "UPLOADED" and worksheet.cell(row_idx, 7).value == "UPLOADED":
            state["isvideouploaded"] = True
            logger.info(f"✅ Row {row_idx} fully synchronized.")

    except Exception as e:
        logger.error(f"Critical Node 5 Failure: {e}", exc_info=True)
        state["isvideouploaded"] = False

    return state