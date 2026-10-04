"""Fetch a small sample of recently played tracks and save the raw response.

Run from the project root:

    python ingestion/fetch_recently_played_sample.py

The first run opens a browser window for Spotify authorization.
The sample is written to data/raw/ (excluded from Git) and can be
explored with ingestion/inspect_recently_played.py.
"""

import json
import os
from pathlib import Path

import spotipy
from dotenv import load_dotenv
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyPKCE


OUTPUT_PATH = Path("data/raw/recently_played_sample.json")


def main():
    load_dotenv()

    client_id = os.environ["SPOTIPY_CLIENT_ID"]
    redirect_uri = os.environ["SPOTIPY_REDIRECT_URI"]

    cache_handler = CacheFileHandler(
        cache_path=".spotify_cache"
    )

    auth_manager = SpotifyPKCE(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope="user-read-recently-played",
        cache_handler=cache_handler,
    )

    spotify = spotipy.Spotify(
        auth_manager=auth_manager
    )

    results = spotify.current_user_recently_played(
        limit=20
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        json.dump(
            results,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print(f"Retrieved {len(results['items'])} listening events.")
    print(f"Raw response saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
