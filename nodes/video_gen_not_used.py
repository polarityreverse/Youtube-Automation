import os
import json
import asyncio
import requests
import time
from google import genai
from google.genai import types
from utils.schema import flowstate
from utils.sheets import get_worksheet
from config import (
    OUTPUT_DIR, PROJECT_ID, LOCATION, VEO_MODEL_NAME
)

# -----------------------------
# WORKER: GENERATE SINGLE CLIP
# -----------------------------
async def generate_veo_clip(client, prompt, duration, filename):
    """Worker function: Generates a single Veo clip with polling."""

    if os.path.exists(filename):
        print(f"✅ Video Cache Hit: {os.path.basename(filename)}")
        return filename

    try:
        # 1. Start generation
        operation = client.models.generate_videos(
            model=VEO_MODEL_NAME,
            prompt=prompt,
            config=types.GenerateVideosConfig(
                aspect_ratio="9:16",
                duration_seconds=int(duration),
            )
        )
        print(f"🚀 Started Gen: {os.path.basename(filename)}...")

        # 2. Polling loop
        start_time = time.time()
        timeout = 600  # 10 minutes max

        while not operation.done:
            if (time.time() - start_time) > timeout:
                print(f"❌ Timeout for {os.path.basename(filename)}")
                return None

            await asyncio.sleep(15)
            operation = client.operations.get(operation)

        # 3. Save video
        if operation.response and operation.response.generated_videos:
            video_data = operation.response.generated_videos[0].video
            video_data.save(filename)
            print(f"💾 Saved: {os.path.basename(filename)}")
            return filename

        print(f"⚠️ Operation finished but no video found for {os.path.basename(filename)}")
        return None

    except Exception as e:
        print(f"❌ Veo Error for {os.path.basename(filename)}: {e}")
        return None


# -----------------------------
# MAIN NODE 3 FUNCTION (FINAL)
# -----------------------------
async def video_generation(state: flowstate) -> flowstate:
    """Node 3: Generate all Veo clips using prompts from Node 1."""

    if not state["isvoicegenerated"]:
        print("⚠️ Aborted: No Audio.")
        return state

    # 1. Initialize Vertex AI client
    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION
    )

    row_id = state["row_index"]
    worksheet = get_worksheet("ideas")
    script_data = state["script"]
    scenes = script_data["scenes"]

    # 2. No Claude refinement — directly use Video_Action_Prompt
    refined_prompts = [scene["Video_Action_Prompt"] for scene in scenes]

    # 3. Generate clips in parallel
    print(f"🎬 Generating {len(refined_prompts)} clips via Veo...")

    tasks = []
    for i, prompt in enumerate(refined_prompts):
        filename = os.path.join(OUTPUT_DIR, f"row_{row_id}_scene_{i+1}.mp4")
        duration = scenes[i].get("Scene_Duration", 5)
        tasks.append(generate_veo_clip(client, prompt, duration, filename))

    results = await asyncio.gather(*tasks)

    # 4. Save results
    state["video_paths"] = [r for r in results if r is not None]
    state["isvideogenerated"] = (len(state["video_paths"]) == len(scenes))

    print(f"✅ Video Node Complete. Total Clips: {len(state['video_paths'])}")
    return state