 # AWS Healthcare & Biotech ML Pipeline

This project is a domain-specific machine learning pipeline built on AWS and centered on the healthcare and biotech sector. The goal is to transform unstructured industry news, market activity, and company-specific events into structured features suitable for predictive modeling. The emphasis is on learning real production-grade ML engineering by constructing an end-to-end pipeline rather than isolated scripts.

The AWS environment is fully initialized, including the CLI configuration, IAM setup, and the primary S3 bucket at `healthcare-ml-pipeline`.

## Current Pipeline

### RSS Ingestion

The pipeline collects biotech and healthcare news each day from sources such as Endpoints, FierceBiotech, PharmaTimes, MedicalXpress, StatNews, and DrugDiscoveryTrends. Articles are pulled through RSS feeds, stored locally as raw JSON, and uploaded to the raw section of the S3 bucket. The ingestion script is intended to be run once per day in the late afternoon so that daily snapshots reflect the complete set of available articles.

### Sentiment Processing

Each article is processed through a sentiment classification pipeline powered by FinBERT. Titles and summaries are combined and cleaned before passing through the model. Each record is labeled with both a sentiment category and a model confidence score. These processed datasets are stored locally as parquet files and are uploaded to the processed section of S3.

### Ticker Mapping

Ticker mapping is applied to the processed sentiment data. Articles are scanned for exact ticker mentions, company names, and commonly used aliases such as “J&J,” “Lilly,” “Novo,” and “BioNTech.” This mapping logic will later expand to link drug names to parent companies. The output is stored both locally and in S3.

### Master Sentiment Dataset

A unified master dataset is maintained inside `data/rss_processed_full`. When new daily sentiment files are created, only new rows are appended. This prevents overwriting and ensures that the dataset evolves into a growing historical record of sentiment, article text, metadata, and mapped tickers. This master dataset will serve as the foundation for feature engineering and modeling.

## Next Steps

The next stage involves merging mapped sentiment with historical stock price data for each covered ticker. This layer will compute daily sentiment aggregates, sentiment momentum, article volumes, rolling statistics, and short-horizon return targets such as one-day, three-day, and five-day forward returns.

Future components will include FDA event alignment, clinical trials data extraction, earnings call analysis, and additional domain-driven features relevant to biotech catalysts. Later stages of the project will develop event-driven return prediction models using methods ranging from tree-based algorithms to sequential models.

## AWS Integration

The system is designed to integrate with AWS services as it matures. S3 functions as the central data lake. Later stages will add AWS Glue for data cataloging, Athena for querying, SageMaker for model training, and Lambda or ECS for scheduled processing. CI/CD support through GitHub Actions can also be added when the pipeline stabilizes.

## Objective

The objective of this project is to build a complete healthcare-focused ML intelligence system that organizes, enriches, and models domain-specific information. This includes NLP, data engineering, cloud infrastructure, and the unique dynamics of biotech markets, all brought together into one coherent pipeline.



