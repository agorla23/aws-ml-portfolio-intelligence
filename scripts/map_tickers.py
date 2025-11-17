import os
import re
import json
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


# -------------------------
# LOAD HEALTHCARE TICKERS
# -------------------------

HEALTHCARE_TICKERS = {
    "PFE": "Pfizer",
    "MRK": "Merck",
    "BMY": "Bristol Myers Squibb",
    "GILD": "Gilead Sciences",
    "AMGN": "Amgen",
    "LLY": "Eli Lilly",
    "REGN": "Regeneron",
    "VRTX": "Vertex",
    "AZN": "AstraZeneca",
    "NVS": "Novartis",
    "SNY": "Sanofi",
    "GSK": "GSK plc",
    "BIIB": "Biogen",
    "ABBV": "AbbVie",
    "INCY": "Incyte",
    "NBIX": "Neurocrine Biosciences",
    "ALNY": "Alnylam Pharmaceuticals",
    "BLUE": "Bluebird Bio",
    "SGEN": "Seagen",
    "FOLD": "Amicus Therapeutics",
    "IONS": "Ionis Pharmaceuticals",
    "SRPT": "Sarepta Therapeutics",
    "EXEL": "Exelixis",
    "CLDX": "Celldex Therapeutics",
    "NVAX": "Novavax",
    "MCRB": "Seres Therapeutics",
    "CRSP": "CRISPR Therapeutics",
    "NTLA": "Intellia Therapeutics",
    "EDIT": "Editas Medicine",
    "BEAM": "Beam Therapeutics",
    "VERV": "Verve Therapeutics",
    "QURE": "UniQure",
    "ARWR": "Arrowhead Pharmaceuticals",
    "HALO": "Halozyme",
    "KYMR": "Kymera Therapeutics",
    "ABUS": "Arbutus Biopharma",
    "SURF": "Surface Oncology",
    "HOOK": "HOOKIPA Pharma",
    "IMCR": "Immunocore",
    "HCM": "HUTCHMED",
    "KNSA": "Kiniksa Pharmaceuticals",
    "NBSE": "NeuBase Therapeutics",
    "DNA": "Ginkgo Bioworks",
    "COYA": "Coya Therapeutics",
    "BCRX": "BioCryst Pharmaceuticals",
    "XLRN": "Acceleron Pharma",
    "ROIV": "Roivant Sciences",
    "VKTX": "Viking Therapeutics",
    "MDGL": "Madrigal Pharmaceuticals",
    "AKRO": "Akero Therapeutics",
    "ALT": "Altimmune",
    "ARDX": "Ardelyx",
    "COLL": "Collegium Pharma",
    "CYTK": "Cytokinetics",
    "RGLS": "Regulus Therapeutics",
    "ALKS": "Alkermes",
    "ZYME": "Zymeworks",
    "ARRY": "Array Biopharma",
    "TGTX": "TG Therapeutics",
    "BPMC": "Blueprint Medicines",
    "AMRN": "Amarin",
    "ACAD": "ACADIA Pharmaceuticals",
    "XENE": "Xenon Pharmaceuticals",
    "CRNX": "Crinetics Pharmaceuticals",
    "SAGE": "Sage Therapeutics",
    "RCKT": "Rocket Pharmaceuticals",
    "MEIP": "MEI Pharma",
    "PRTA": "Prothena",
    "RPRX": "Royalty Pharma",
    "DXCM": "Dexcom",
    "TMO": "Thermo Fisher",
    "ILMN": "Illumina",
    "PACB": "Pacific Biosciences",
    "TECH": "Bio-Techne",
    "WAT": "Waters Corp.",
    "BAX": "Baxter",
    "BDX": "Becton Dickinson",
    "ABT": "Abbott",
    "ISRG": "Intuitive Surgical",
    "ZBH": "Zimmer Biomet",
    "STE": "STERIS",
    "EW": "Edwards Lifesciences",
    "HOLX": "Hologic",
    "JNJ": "Johnson & Johnson",
    "NVO": "Novo Nordisk",
    "BNTX": "BioNTech"
}

# -------------------------
# ADDITIONAL COMPANY NAME ALIASES
# -------------------------

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


# -------------------------
# MATCH TICKERS
# -------------------------

def find_tickers(text):
    if not isinstance(text, str):
        return []

    text_lower = text.lower()
    found = set()

    # 1. Exact ticker matches
    for ticker in HEALTHCARE_TICKERS.keys():
        pattern = r"\b" + re.escape(ticker.lower()) + r"\b"
        if re.search(pattern, text_lower):
            found.add(ticker)

    # 2. Company name matches
    for name, ticker in COMPANY_NAMES.items():
        if name in text_lower:
            found.add(ticker)

    # 3. Alias matches
    for alias, ticker in ALIAS_MAP.items():
        if alias in text_lower:
            found.add(ticker)

    return list(found)

# -------------------------
# MAIN PROCESSOR
# -------------------------

def map_tickers(date_str=None):
    ensure_dir(MAPPED_DIR)

    if date_str is None:
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    input_path = os.path.join(PROCESSED_DIR, f"rss_processed_{date_str}.parquet")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Processed file not found: {input_path}")

    print(f"Loading {input_path}")
    df = pd.read_parquet(input_path)

    print("Running ticker matching...")
    df["tickers"] = df["full_text"].apply(find_tickers)

    output_path = os.path.join(MAPPED_DIR, f"rss_mapped_{date_str}.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Saved mapped parquet: {output_path}")

    s3_path = f"s3://{BUCKET}/processed/rss_mapped/{date_str}.parquet"
    upload_to_s3(output_path, s3_path)
    print(f"Uploaded: {s3_path}")


if __name__ == "__main__":
    map_tickers()
