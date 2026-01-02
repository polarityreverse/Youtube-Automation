from utils.youtube_auth import get_youtube_client # Assuming your script is here

def get_performance_context():
    #Fetches Top 5 stats from YT and IG for LLM context.
    youtube = get_youtube_client()
    context_str = "Recent High-Performing Topics:\n"

    # --- YouTube Top 5 ---
    try:
        # Search for your channel's top videos by view count
        request = youtube.search().list(
            part="snippet",
            forMine=True,
            maxResults=5,
            order="viewCount",
            type="video"
        )
        response = request.execute()
        
        context_str += "YouTube Successes:\n"
        for item in response.get('items', []):
            title = item['snippet']['title']
            v_id = item['id']['videoId']
            # Get specific view counts
            v_stats = youtube.videos().list(part="statistics", id=v_id).execute()
            views = v_stats['items'][0]['statistics'].get('viewCount', 0)
            context_str += f"- {title} ({views} views)\n"
    except Exception as e:
        print(f"YT Stats Error: {e}")

    return context_str
    