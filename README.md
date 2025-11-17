 AWS Healthcare & Biotech ML Pipeline

This project is a **domain-specific machine learning pipeline** built on AWS, focused entirely on the **healthcare and biotech** sector.  
The system ingests healthcare news, processes it into sentiment-aware features, maps content to public biotech/pharma tickers, and prepares the foundation for event-driven price prediction models.

The goal is not to build a toy ML script — the goal is to learn **real, production-grade ML engineering** by building a functioning cloud pipeline.

---

##  Current Status (Up to Date)

### ✔ AWS Environment Set Up
- AWS CLI configured  
- IAM access configured  
- S3 bucket created: `s3://healthcare-ml-pipeline/`

---

### Phase 1 — Healthcare RSS Ingestion Layer
A daily ingestion script pulls domain-specific news from:

- Endpoints News  
- FierceBiotech  
- PharmaTimes  
- StatNews  
- MedicalXpress  
- DrugDiscoveryTrends  

Raw articles are saved as JSON and uploaded to S3:


---

### ✔ Phase 2 — Sentiment Processing (FinBERT)
- Cleans article text  
- Merges title + summary  
- Runs **FinBERT** (financial-tone sentiment model)  
- Outputs:
  - `sentiment_label` (positive/neutral/negative)  
  - `sentiment_score` (model confidence)  

Processed data is uploaded to:


---

### ✔ Phase 3 — Ticker Mapping Layer
Each article is mapped to public healthcare tickers using:

- exact ticker matching  
- company name matching  
- alias recognition (e.g., “J&J”, “Lilly”, “BioNTech”, “Novo”)  
- early drug-to-ticker mapping (Keytruda → MRK, Opdivo → BMY, Ozempic → NVO, etc.)

Outputs are saved to:

##  Planned Architecture (Next Steps)

### **Feature Engineering Layer**
- Merge mapped sentiment with historical stock data  
- Compute daily + rolling sentiment statistics  
- Build sentiment momentum, article counts, and volatility features  
- Create short-term forward return targets (1D, 3D, 5D)

### **Domain Event Layer**
- FDA calendar integration  
- ClinicalTrials.gov data extraction  
- Earnings call sentiment  
- Catalyst proximity features

### **Modeling Layer**
- Event-driven return prediction  
- Gradient Boosting / LSTMs / Transformers  
- Rolling-window training pipelines  

### **AWS Integration**
- S3 raw/processed/features structure  
- AWS Glue catalog + Athena queries  
- SageMaker training jobs  
- Lambda/ECS for automation  
- Optional GitHub Actions for CI/CD

---
## Project Objective

To build a fully functioning **healthcare-focused ML intelligence system** capable of turning unstructured biotech news and medical events into structured signals for predictive modeling.

This project blends:

- NLP  
- cloud architecture  
- healthcare information  
- event-driven ML  
- real-world data engineering  

into one unified pipeline.

---


