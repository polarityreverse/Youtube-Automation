import os
from dotenv import load_dotenv

load_dotenv()

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
GEMINI_API_KEY_1 = os.getenv("GEMINI_API_KEY_1")
GEMINI_API_KEY_2 = os.getenv("GEMINI_API_KEY_2")
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")
LUMA_API_KEY = os.getenv("LUMA_API_KEY")
VEO_API_KEY = os.getenv("VEO_API_KEY")

IDEA_GENERATION_MODEL = "gemini-2.5-flash-preview-09-2025"
VIDEO_METADATA_GENERATION_MODEL = "gemini-2.0-flash"
ELEVENLABS_MODEL = "eleven_multilingual_v2"
IMAGEN_MODEL = "imagen-4.0-ultra-generate-001"
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"
VEO_MODEL_NAME = "veo-3.1-generate-001"

PROJECT_ID = "ankit-demo-cli"
LOCATION = "us-central1"

TIKTOK_CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY")
TIKTOK_CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET")
TIKTOK_ACCESS_TOKEN =  os.getenv("TIKTOK_ACCESS_TOKEN")
INSTA_ACCESS_TOKEN = os.getenv("INSTA_ACCESS_TOKEN")
INSTA_ACCOUNT_ID = os.getenv("INSTA_ACCOUNT_ID")

VOICE_IDS = [v.strip() for v in os.getenv("ELEVENLABS_VOICE_IDS", "").split(",") if v.strip()]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "assets")
os.makedirs(OUTPUT_DIR, exist_ok=True)

IDEA_GENERATION_API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{IDEA_GENERATION_MODEL}:generateContent?key={GEMINI_API_KEY_1}"
)

"""
SCRIPT_GENERATION_API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{SCRIPT_GENERATION_MODEL}:generateContent?key={GEMINI_API_KEY}"
)
"""

ELEVENLABS_VOICE_GENERATION_API_URL = (
    f"https://api.elevenlabs.io/v1/text-to-speech"
)

CLAUDE_SCRIPT_IMAGE_PROMPT_URL = (
    f"https://api.anthropic.com/v1/messages"
)

IMAGEN_IMAGE_GENERATION_API_URL_1 = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{IMAGEN_MODEL}:predict?key={GEMINI_API_KEY_1}"
)

IMAGEN_IMAGE_GENERATION_API_URL_2 = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{IMAGEN_MODEL}:predict?key={GEMINI_API_KEY_2}"
)

LUMA_VIDEO_GENERATION_API_URL = (
    f"https://api.lumalabs.ai/dream-machine/v1/generations"
)

GITHUB_RAW_BASE = (
    f"https://raw.githubusercontent.com/polarityreverse/doc-assets/master/output_assets/"
)


TIKTOK_REDIRECT_URI="https://thea-noncoagulating-photographically.ngrok-free.dev/callback"


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPTS_DIR = os.path.join(BASE_DIR, "prompts")

def load_prompt(filename):
    with open(os.path.join(PROMPTS_DIR, filename), "r", encoding="utf-8") as f:
        return f.read()


# Load prompts from .txt files
IDEA_SYSTEM_INSTRUCTIONS = load_prompt("idea_system_instructions.txt")
CLAUDE_SYSTEM_PROMPT = load_prompt("claude_system_prompt.txt")
SCRIPT_GENERATION_PROMPT = load_prompt("script_generation_prompt.txt")
SCRIPT_GENERATION_SYSTEM_INSTRUCTIONS = load_prompt("script_system_instructions.txt")
LUMA_MOTION_BASE = load_prompt("luma_motion_base.txt")

