import os
import datetime
import pandas as pd
import re
import subprocess

MASTER_DIR = "data/rss_processed_full"
MASTER_FILE = "sentiment_full.parquet"
OUTPUT_FILE = "sentiment_full_with_tickers.parquet"

BUCKET = "healthcare-ml-pipeline"

# ----------------------------
# Ensure directories exist
# ----------------------------
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def upload_to_s3(local_path, s3_path):
    """Upload file to S3."""
    subprocess.run(["aws", "s3", "cp", local_path, s3_path])


# ----------------------------
# Ticker + Alias Dictionaries
# ----------------------------
HEALTHCARE_TICKERS = {
    "PFE": "Pfizer", "MRK": "Merck", "BMY": "Bristol Myers Squibb",
    "GILD": "Gilead Sciences", "AMGN": "Amgen", "LLY": "Eli Lilly",
    "REGN": "Regeneron", "VRTX": "Vertex", "AZN": "AstraZeneca",
    "NVS": "Novartis", "SNY": "Sanofi", "GSK": "GSK plc",
    "BIIB": "Biogen", "ABBV": "AbbVie", "INCY": "Incyte",
    "NBIX": "Neurocrine Biosciences", "ALNY": "Alnylam Pharmaceuticals",
    "BLUE": "Bluebird Bio", "SGEN": "Seagen", "FOLD": "Amicus Therapeutics",
    "IONS": "Ionis Pharmaceuticals", "SRPT": "Sarepta Therapeutics",
    "EXEL": "Exelixis", "CLDX": "Celldex Therapeutics",
    "NVAX": "Novavax", "MCRB": "Seres Therapeutics",
    "CRSP": "CRISPR Therapeutics", "NTLA": "Intellia Therapeutics",
    "EDIT": "Editas Medicine", "BEAM": "Beam Therapeutics",
    "VERV": "Verve Therapeutics", "QURE": "UniQure",
    "ARWR": "Arrowhead Pharmaceuticals", "HALO": "Halozyme",
    "KYMR": "Kymera Therapeutics", "ABUS": "Arbutus Biopharma",
    "SURF": "Surface Oncology", "HOOK": "HOOKIPA Pharma",
    "IMCR": "Immunocore", "HCM": "HUTCHMED", "KNSA": "Kiniksa Pharmaceuticals",
    "NBSE": "NeuBase Therapeutics", "DNA": "Ginkgo Bioworks",
    "COYA": "Coya Therapeutics", "BCRX": "BioCryst Pharmaceuticals",
    "XLRN": "Acceleron Pharma", "ROIV": "Roivant Sciences",
    "VKTX": "Viking Therapeutics", "MDGL": "Madrigal Pharmaceuticals",
    "AKRO": "Akero Therapeutics", "ALT": "Altimmune",
    "ARDX": "Ardelyx", "COLL": "Collegium Pharma",
    "CYTK": "Cytokinetics", "RGLS": "Regulus Therapeutics",
    "ALKS": "Alkermes", "ZYME": "Zymeworks",
    "ARRY": "Array Biopharma", "TGTX": "TG Therapeutics",
    "BPMC": "Blueprint Medicines", "AMRN": "Amarin",
    "ACAD": "ACADIA Pharmaceuticals", "XENE": "Xenon Pharmaceuticals",
    "CRNX": "Crinetics Pharmaceuticals", "SAGE": "Sage Therapeutics",
    "RCKT": "Rocket Pharmaceuticals", "MEIP": "MEI Pharma",
    "PRTA": "Prothena", "RPRX": "Royalty Pharma",
    "DXCM": "Dexcom", "TMO": "Thermo Fisher", "ILMN": "Illumina",
    "PACB": "Pacific Biosciences", "TECH": "Bio-Techne",
    "WAT": "Waters Corp.", "BAX": "Baxter",
    "BDX": "Becton Dickinson", "ABT": "Abbott",
    "ISRG": "Intuitive Surgical", "ZBH": "Zimmer Biomet",
    "STE": "STERIS", "EW": "Edwards Lifesciences",
    "HOLX": "Hologic", "JNJ": "Johnson & Johnson",
    "NVO": "Novo Nordisk", "BNTX": "BioNTech"
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


# ----------------------------
# Ticker Matching Function
# ----------------------------
def find_tickers(text):
    if not isinstance(text, str):
        return []

    txt = text.lower()
    found = set()

    # Exact ticker match
    for ticker in HEALTHCARE_TICKERS.keys():
        pattern = r"\b" + ticker.lower() + r"\b"
        if re.search(pattern, txt):
            found.add(ticker)

    # Company name match
    for name, ticker in COMPANY_NAMES.items():
        if name in txt:
            found.add(ticker)

    # Aliases
    for alias, ticker in ALIAS_MAP.items():
        if alias in txt:
            found.add(ticker)

    return list(found)


# ----------------------------
# Main: Build Full Ticker-Mapped Dataset
# ----------------------------
def update_master():
    ensure_dir(MASTER_DIR)

    master_path = os.path.join(MASTER_DIR, MASTER_FILE)
    output_path = os.path.join(MASTER_DIR, OUTPUT_FILE)

    print(f"Loading master sentiment dataset:\n  {master_path}")
    df = pd.read_parquet(master_path)

    print(f"Rows before dedupe: {len(df)}")
    df = df.drop_duplicates(subset=["title", "published"], keep="first")
    print(f"Rows after dedupe: {len(df)}")

    print("Running ticker extraction across full dataset...")
    df["tickers"] = df["full_text"].apply(find_tickers)

    # Save fully combined dataset
    df.to_parquet(output_path, index=False)
    print(f"Saved full sentiment+ticker dataset:\n  {output_path}")

    # Upload to S3
    s3_path = f"s3://{BUCKET}/processed/sentiment/{OUTPUT_FILE}"
    upload_to_s3(output_path, s3_path)
    print(f"Uploaded to S3:\n  {s3_path}")


if __name__ == "__main__":
    update_master()
