import os
import json
import datetime
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import subprocess

RAW_DIR = "data/rss_raw"
PROCESSED_DIR = "data/rss_processed"

# Your S3 bucket
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
    """Run FinBERT sentiment on a piece of text."""
    if not isinstance(text, str) or len(text.strip()) == 0:
        return None, None

    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)

    with torch.no_grad():
        outputs = model(**inputs)

    logits = outputs.logits
    probs = torch.softmax(logits, dim=1).numpy()[0]

    labels = ["negative", "neutral", "positive"]
    sentiment_label = labels[probs.argmax()]
    sentiment_score = float(probs.max())  # confidence score

    return sentiment_label, sentiment_score


# -----------------------------
# MAIN PROCESSOR
# -----------------------------
def process_raw_rss(date_str=None):
    ensure_dir(PROCESSED_DIR)

    if date_str is None:
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    raw_path = os.path.join(RAW_DIR, f"rss_raw_{date_str}.json")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    print(f"Processing: {raw_path}")

    df = pd.read_json(raw_path)

    # Combine title + summary for better sentiment signal
    df["full_text"] = df["title"].astype(str) + ". " + df["summary"].astype(str)

    # Clean whitespace
    df["full_text"] = df["full_text"].str.replace("\n", " ", regex=False).str.strip()

    sentiments = []
    scores = []

    for text in df["full_text"]:
        label, score = compute_sentiment(text)
        sentiments.append(label)
        scores.append(score)

    df["sentiment_label"] = sentiments
    df["sentiment_score"] = scores

    # Save processed parquet
    processed_path = os.path.join(PROCESSED_DIR, f"rss_processed_{date_str}.parquet")
    df.to_parquet(processed_path, index=False)

    print(f"Saved processed parquet: {processed_path}")

    # Upload to S3
    s3_path = f"s3://{BUCKET}/processed/rss/{date_str}.parquet"
    upload_to_s3(processed_path, s3_path)

    print(f"Uploaded processed data to: {s3_path}")


if __name__ == "__main__":
    process_raw_rss()
