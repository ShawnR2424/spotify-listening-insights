"""Print the structure of a saved recently-played API response.

Run from the project root:

    python ingestion/inspect_recently_played.py [path/to/sample.json]

With no argument, reads the sample created by
ingestion/fetch_recently_played_sample.py.
"""

import json
import sys
from pathlib import Path


DEFAULT_SAMPLE_PATH = Path("data/raw/recently_played_sample.json")


def print_keys(title, keys):
    print(f"\n{title}")
    print("-" * len(title))
    for key in keys:
        print(key)


def main():
    file_path = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else DEFAULT_SAMPLE_PATH
    )

    if not file_path.exists():
        sys.exit(
            f"Sample file not found: {file_path}\n"
            "Create one with: "
            "python ingestion/fetch_recently_played_sample.py"
        )

    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    print_keys("TOP-LEVEL KEYS", data.keys())

    print("\nNUMBER OF LISTENING EVENTS")
    print("--------------------------")
    print(len(data["items"]))

    if not data["items"]:
        print("\nNo listening events in the sample.")
        return

    first_item = data["items"][0]

    print_keys("LISTENING EVENT KEYS", first_item.keys())
    print_keys("TRACK KEYS", first_item["track"].keys())
    print_keys("ALBUM KEYS", first_item["track"]["album"].keys())
    print_keys("ARTIST KEYS", first_item["track"]["artists"][0].keys())


if __name__ == "__main__":
    main()
