
import yfinance as yf
import boto3
from datetime import date

# --- Configuration ---
BUCKET_NAME = "akhil-ml-pipeline-data"      # your S3 bucket name
RAW_PREFIX = "raw/market_data/"             # folder path in the bucket
TICKERS = ["SPY", "AAPL", "MSFT", "GOOG"]  # which stocks to fetch

# --- AWS client setup ---
s3 = boto3.client("s3")  # connects to AWS using credentials from aws configure

# --- Date for versioning filenames ---
today = date.today().isoformat()

# --- Loop through each stock ticker ---
for ticker in TICKERS:
    print(f"\n--- Starting download for {ticker} ---")

    # 1️⃣ Download 1 month of daily data
    df = yf.download(ticker, period="1mo", interval="1d")
    print(f"Downloaded {len(df)} rows for {ticker}")

    # 2️⃣ Save locally
    local_filename = f"{ticker}_{today}.csv"
    df.to_csv(local_filename)
    print(f"Saved file locally: {local_filename}")

    # 3️⃣ Upload to S3
    s3_key = f"{RAW_PREFIX}{local_filename}"
    try:
        s3.upload_file(local_filename, BUCKET_NAME, s3_key)
        print(f"✅ Uploaded to s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"❌ Upload failed for {ticker}: {e}")
