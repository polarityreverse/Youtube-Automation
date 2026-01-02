from typing import TypedDict, List, Dict, Any

class flowstate(TypedDict):
    # Tracking
    row_index: int              # The Google Sheet row we are working on
    idea: str                   # The science topic (e.g., "Why tea stays hot")
    
    # Generated Content
    script: Dict[str, Any]      # The JSON script from Node 1
    image_paths: List[str]      # Paths to local Imagen 3 stills from Node 3
    video_paths: List[str]      # Paths to local Luma MP4s from Node 4
    vo_path: str                # Path to the final ElevenLabs voiceover
    alignment_data: Dict[str, Any]  # The character-level timestamps from ElevenLabs
    
    # Status Flags
    isscriptgenerated: bool
    isvoicegenerated: bool
    isimagesgenerated: bool
    isvideogenerated: bool

