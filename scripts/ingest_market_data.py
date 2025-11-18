import os
import datetime
import pandas as pd
import yfinance as yf
import subprocess
import numpy as np

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TICKERS = [
    "ABBV", "ALKS", "ALT", "AMGN", "AZN",
    "BMY", "BNTX", "JNJ", "LLY", "MRK",
    "NVO", "PFE", "SNY", "TMO"
]

OUTPUT_DIR = "data/market_raw"
COMBINED_PATH = "data/market_data_features.parquet"
BUCKET = "healthcare-ml-pipeline"


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def upload_to_s3(local_path, s3_path):
    subprocess.run(["aws", "s3", "cp", local_path, s3_path])


# --------------------------------------------------
# FEATURE ENGINEERING
# --------------------------------------------------
def engineer_features(df):
    df = df.copy()

    df["return"] = df["Adj Close"].pct_change()
    df["log_return"] = np.log(df["Adj Close"] / df["Adj Close"].shift(1))

    df["vol_20d"] = df["return"].rolling(20).std()
    df["ma_10"] = df["Adj Close"].rolling(10).mean()
    df["ma_50"] = df["Adj Close"].rolling(50).mean()
    df["mom_10"] = df["Adj Close"] / df["Adj Close"].shift(10) - 1

    df["ticker"] = df["Ticker"]
    return df



# --------------------------------------------------
# MAIN INGESTION
# --------------------------------------------------
def run_ingestion():
    ensure_dir(OUTPUT_DIR)

    all_frames = []

    print("\nStarting market data ingestion...\n")

    for ticker in TICKERS:
        print(f"Pulling {ticker}...")

        try:
            data = yf.download(ticker, period="5y", auto_adjust=False, progress=False)
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            continue

        if data.empty:
            print(f"No data for {ticker}")
            continue

        data["Ticker"] = ticker

        # Feature engineering
        data = engineer_features(data)

        # Save to local parquet
        out_path = os.path.join(OUTPUT_DIR, f"{ticker}.parquet")
        data.to_parquet(out_path)
        print(f"Saved: {out_path}")

        # Upload to S3
        s3_path = f"s3://{BUCKET}/raw/market/{ticker}.parquet"
        upload_to_s3(out_path, s3_path)
        print(f"Uploaded: {s3_path}")

        all_frames.append(data.reset_index())

    # Merge all tickers into a single dataset
    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined.to_parquet(COMBINED_PATH)
        print(f"\nSaved combined market data: {COMBINED_PATH}")

        upload_to_s3(
            COMBINED_PATH,
            f"s3://{BUCKET}/processed/market_data_features.parquet"
        )
        print("Uploaded combined market data to S3")
    else:
        print("No data collected — nothing to merge.")


if __name__ == "__main__":
    run_ingestion()
