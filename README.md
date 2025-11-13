# AWS Portfolio Intelligence System

This project is an end-to-end AWS-based machine learning pipeline designed to ingest financial market data, process it into model-ready features, and (later) deliver portfolio insights, risk metrics, and sentiment-aware analytics.

The goal is to learn real, production-grade cloud ML engineering by building a functioning system, not just isolated scripts.

---

## 🚀 Current Status (Phase 1 Complete)
- AWS CLI + IAM configuration completed  
- S3 bucket created  
- Data ingestion script (`ingest_market_data.py`) working  
  - Downloads SPY, AAPL, MSFT, GOOG  
  - Saves local CSV  
  - Uploads to S3 under `raw/market_data/`  

Next step: **Processing Layer** — merge raw files, compute returns + volatility, save processed dataset to S3.

---

## 🧱 Planned Architecture
- **Ingestion Layer:** yfinance → S3  
- **Processing Layer:** returns, volatility, correlations  
- **Analytics Layer:** risk metrics, portfolio optimization  
- **NLP Layer:** market sentiment integration  
- **Deployment:** Lambda/API (later)  
- **Automation:** CloudWatch or Step Functions  

A more detailed architecture diagram will be added as the project grows.

---

## 🗂️ Repository Structure
