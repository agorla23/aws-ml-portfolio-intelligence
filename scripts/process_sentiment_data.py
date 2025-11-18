import os
import json
import datetime
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import subprocess

RAW_DIR = "data/rss_raw"
PROCESSED_DIR = "data/rss_processed"
FULL_DIR = "data/rss_processed_full"

BUCKET = "healthcare-ml-pipeline"


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def upload_to_s3(local_path, s3_path):
    subprocess.run(["aws", "s3", "cp", local_path, s3_path])


# -----------------------------
# LOAD FINBERT
# -----------------------------
print("Loading FinBERT model...")

tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
model.eval()


def compute_sentiment(text):
    """Run FinBERT sentiment on text."""
    if not isinstance(text, str) or len(text.strip()) == 0:
        return None, None

    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)

    with torch.no_grad():
        outputs = model(**inputs)

    logits = outputs.logits
    probs = torch.softmax(logits, dim=1).numpy()[0]

    labels = ["negative", "neutral", "positive"]
    sentiment_label = labels[probs.argmax()]
    sentiment_score = float(probs.max())

    return sentiment_label, sentiment_score


# -----------------------------
# MAIN PROCESSOR + APPEND LOGIC
# -----------------------------
def process_raw_rss(date_str=None):
    ensure_dir(PROCESSED_DIR)
    ensure_dir(FULL_DIR)

    if date_str is None:
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    raw_path = os.path.join(RAW_DIR, f"rss_raw_{date_str}.json")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    print(f"Processing: {raw_path}")

    df = pd.read_json(raw_path)

    # Full text = title + summary
    df["full_text"] = df["title"].astype(str) + ". " + df["summary"].astype(str)
    df["full_text"] = df["full_text"].str.replace("\n", " ", regex=False).str.strip()

    sentiments = []
    scores = []

    for text in df["full_text"]:
        label, score = compute_sentiment(text)
        sentiments.append(label)
        scores.append(score)

    df["sentiment_label"] = sentiments
    df["sentiment_score"] = scores

    # Save daily processed file
    processed_path = os.path.join(PROCESSED_DIR, f"rss_processed_{date_str}.parquet")
    df.to_parquet(processed_path, index=False)
    print(f"Saved processed parquet: {processed_path}")

    # Upload daily to S3
    upload_to_s3(processed_path, f"s3://{BUCKET}/processed/rss/{date_str}.parquet")
    print("Uploaded processed daily file to S3.")

    # -----------------------------
    # APPEND INTO FULL MASTER DATASET
    # -----------------------------
    full_path = os.path.join(FULL_DIR, "sentiment_full.parquet")

    if os.path.exists(full_path):
        print("Loading existing full sentiment dataset...")
        full_df = pd.read_parquet(full_path)
    else:
        print("No full sentiment dataset found — creating new one.")
        full_df = pd.DataFrame(columns=df.columns)

    # Append
    combined = pd.concat([full_df, df], ignore_index=True)

    # Deduplicate by link
    combined = combined.drop_duplicates(subset=["link"])

    # Save updated full dataset
    combined.to_parquet(full_path, index=False)
    print(f"Updated full dataset saved: {full_path}")

    # Upload full dataset to S3
    upload_to_s3(full_path, f"s3://{BUCKET}/processed/sentiment_full.parquet")
    print("Uploaded full master dataset to S3.")

    print("DONE.")


if __name__ == "__main__":
    process_raw_rss()
