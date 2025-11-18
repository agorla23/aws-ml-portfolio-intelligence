import os
import pandas as pd
import datetime
import subprocess

# --------------------------------------------------------
# CONFIG
# --------------------------------------------------------

PROCESSED_SENTIMENT_DIR = "data/rss_mapped"
MARKET_DATA_FILE = "data/market_data_features.parquet"
OUTPUT_DIR = "data/features"
BUCKET = "healthcare-ml-pipeline"


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def upload_to_s3(local_path, s3_path):
    subprocess.run(["aws", "s3", "cp", local_path, s3_path])


# --------------------------------------------------------
# BUILD DAILY SENTIMENT FEATURES
# --------------------------------------------------------

def build_daily_sentiment(sentiment_df):
    print("Building daily sentiment features...")

    # Robust mixed-format date parsing
    sentiment_df["date"] = pd.to_datetime(
        sentiment_df["published"],
        format="mixed",
        errors="coerce"
    ).dt.date

    # Drop rows that couldn't parse
    sentiment_df = sentiment_df.dropna(subset=["date"])

    rows = []

    # Find all unique tickers mentioned
    unique_tickers = sorted(set(
        t for lst in sentiment_df["tickers"] for t in lst if lst
    ))

    for ticker in unique_tickers:
        df_t = sentiment_df[sentiment_df["tickers"].apply(lambda lst: ticker in lst)]

        agg = df_t.groupby("date").agg(
            mean_sentiment=("sentiment_score", "mean"),
            median_sentiment=("sentiment_score", "median"),
            sentiment_std=("sentiment_score", "std"),
            article_count=("sentiment_score", "count")
        ).reset_index()

        agg["ticker"] = ticker

        # Add 1-day momentum
        agg = agg.sort_values("date")
        agg["sentiment_momentum_1d"] = agg["mean_sentiment"].diff()

        rows.append(agg)

    if len(rows) == 0:
        print("⚠️ No sentiment aggregated — no tickers found.")
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)


# --------------------------------------------------------
# MERGE MARKET + SENTIMENT FEATURES
# --------------------------------------------------------

def merge_with_market(sentiment_daily, market_df):
    print("Merging sentiment with market features...")

    # Convert market index to a column
    market_df = market_df.reset_index()

    # Extract date as Python date
    market_df["date"] = market_df["Date"].dt.date

    # Merge
    merged = pd.merge(
        market_df,
        sentiment_daily,
        on=["ticker", "date"],
        how="left"
    )

    # Fill missing sentiment with neutral values
    merged["mean_sentiment"] = merged["mean_sentiment"].fillna(0.0)
    merged["median_sentiment"] = merged["median_sentiment"].fillna(0.0)
    merged["sentiment_std"] = merged["sentiment_std"].fillna(0.0)
    merged["article_count"] = merged["article_count"].fillna(0)
    merged["sentiment_momentum_1d"] = merged["sentiment_momentum_1d"].fillna(0.0)

    return merged


# --------------------------------------------------------
# MAIN
# --------------------------------------------------------

def build_features(date_str=None):
    ensure_dir(OUTPUT_DIR)

    if date_str is None:
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    sentiment_path = os.path.join(
        PROCESSED_SENTIMENT_DIR, f"rss_mapped_{date_str}.parquet"
    )
    print(f"Loading sentiment: {sentiment_path}")

    sentiment_df = pd.read_parquet(sentiment_path)

    print(f"Loading market data: {MARKET_DATA_FILE}")
    market_df = pd.read_parquet(MARKET_DATA_FILE)

    # Build sentiment aggregates
    sentiment_daily = build_daily_sentiment(sentiment_df)

    # Merge with market features
    final_df = merge_with_market(sentiment_daily, market_df)

    # Save
    output_path = os.path.join(OUTPUT_DIR, f"features_{date_str}.parquet")
    final_df.to_parquet(output_path, index=False)

    print(f"Saved final merged feature set: {output_path}")

    # Upload to S3
    s3_path = f"s3://{BUCKET}/processed/features/{date_str}.parquet"
    upload_to_s3(output_path, s3_path)
    print(f"Uploaded to S3: {s3_path}")


if __name__ == "__main__":
    build_features()
