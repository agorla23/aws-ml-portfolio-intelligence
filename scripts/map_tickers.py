import os
import re
import datetime
import pandas as pd
import subprocess

PROCESSED_DIR = "data/rss_processed"
MAPPED_DIR = "data/rss_mapped"
BUCKET = "healthcare-ml-pipeline"

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def upload_to_s3(local_path, s3_path):
    subprocess.run(["aws", "s3", "cp", local_path, s3_path])

# ---------------------------------------
# FULL HEALTHCARE TICKER LIST
# ---------------------------------------

HEALTHCARE_TICKERS = {
    # (same big dictionary you already have)
}

ALIAS_MAP = {
    "j&j": "JNJ",
    "johnson & johnson": "JNJ",
    "johnson and johnson": "JNJ",
    "lilly": "LLY",
    "eli lilly": "LLY",
    "sanofi": "SNY",
    "novartis": "NVS",
    "novo": "NVO",
    "novo nordisk": "NVO",
    "biontech": "BNTX",
    "bio n tech": "BNTX",
}

COMPANY_NAMES = {v.lower(): k for k, v in HEALTHCARE_TICKERS.items()}

# ---------------------------------------
# TICKER EXTRACTION
# ---------------------------------------

def find_tickers(text):
    if not isinstance(text, str):
        return []

    text_lower = text.lower()
    found = set()

    # 1. Exact ticker match
    for ticker in HEALTHCARE_TICKERS.keys():
        pattern = r"\b" + re.escape(ticker.lower()) + r"\b"
        if re.search(pattern, text_lower):
            found.add(ticker)

    # 2. Company name match
    for name, ticker in COMPANY_NAMES.items():
        if name in text_lower:
            found.add(ticker)

    # 3. Alias phrases
    for alias, ticker in ALIAS_MAP.items():
        if alias in text_lower:
            found.add(ticker)

    return list(found)

# ---------------------------------------
# MAIN PROCESSOR
# ---------------------------------------

def map_tickers(date_str=None):
    ensure_dir(MAPPED_DIR)

    # -----------------------------------
    # NEW: allow mapping entire dataset
    # -----------------------------------
    if date_str == "all":
        input_path = os.path.join(PROCESSED_DIR, "rss_processed_all.parquet")
        output_path = os.path.join(MAPPED_DIR, "rss_mapped_all.parquet")
        s3_path = f"s3://{BUCKET}/processed/rss_mapped/all.parquet"

    else:
        if date_str is None:
            date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

        input_path = os.path.join(PROCESSED_DIR, f"rss_processed_{date_str}.parquet")
        output_path = os.path.join(MAPPED_DIR, f"rss_mapped_{date_str}.parquet")
        s3_path = f"s3://{BUCKET}/processed/rss_mapped/{date_str}.parquet"

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Processed file not found: {input_path}")

    print(f"Loading {input_path} ...")
    df = pd.read_parquet(input_path)

    print("Running ticker matching...")
    df["tickers"] = df["full_text"].apply(find_tickers)

    df.to_parquet(output_path, index=False)
    print(f"Saved mapped file: {output_path}")

    upload_to_s3(output_path, s3_path)
    print(f"Uploaded to: {s3_path}")

# ---------------------------------------

if __name__ == "__main__":
    # To run: python map_tickers.py all
    import sys
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    map_tickers(arg)
