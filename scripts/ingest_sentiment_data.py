import os
import datetime
import feedparser
import pandas as pd
import requests

# ------------------------------------------
# CONFIG
# ------------------------------------------

# Where you want the script to save raw data locally
OUTPUT_DIR = "data/rss_raw"

# RSS feeds to pull
RSS_FEEDS = {
    "endpoints": "https://endpts.com/feed/",
    "fiercebiotech": "https://www.fiercebiotech.com/rss.xml",
    "pharmatimes": "https://www.pharmatimes.com/rss/",
    "medicalxpress": "https://medicalxpress.com/rss-feed/",
    "statnews": "https://www.statnews.com/feed/",
    "drugdiscovery": "https://www.drugdiscoverytrends.com/feed/",
}

# ------------------------------------------
# HELPER: ensure directories exist
# ------------------------------------------

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


# ------------------------------------------
# MAIN: pull and store all feeds
# ------------------------------------------


def fetch_feed(name, url):
    print(f"Pulling feed: {name} ...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    }

    # Step 1: fetch RSS XML using requests (not feedparser)
    resp = requests.get(url, headers=headers, timeout=10)

    if resp.status_code != 200:
        print(f"Failed to fetch {name}: HTTP {resp.status_code}")
        return pd.DataFrame()

    xml_data = resp.text

    # Step 2: feedparser consumes the XML string instead of making the request
    feed = feedparser.parse(xml_data)

    print(f"Entries found for {name}: {len(feed.entries)}")

    rows = []
    for entry in feed.entries:
        rows.append({
            "source": name,
            "title": entry.get("title", ""),
            "summary": entry.get("summary", ""),
            "published": entry.get("published", ""),
            "link": entry.get("link", ""),
            "pulled_at": datetime.datetime.utcnow().isoformat()
        })

    return pd.DataFrame(rows)


def run_ingestion():
    """
    Loop through all RSS feeds and save a combined JSON file.
    """
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    ensure_dir(OUTPUT_DIR)

    all_frames = []

    for name, url in RSS_FEEDS.items():
        try:
            df = fetch_feed(name, url)
            all_frames.append(df)
        except Exception as e:
            print(f"Error pulling {name}: {e}")

    if not all_frames:
        print("No RSS feeds could be pulled. Exiting.")
        return

    final_df = pd.concat(all_frames, ignore_index=True)

    # Save to JSON (raw format)
    output_path = os.path.join(OUTPUT_DIR, f"rss_raw_{today}.json")
    final_df.to_json(output_path, orient="records", indent=2)

    print(f"Saved RSS data to: {output_path}")
    print(f"Total articles ingested: {len(final_df)}")


if __name__ == "__main__":
    run_ingestion()
