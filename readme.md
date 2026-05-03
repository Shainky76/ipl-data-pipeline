# IPL Data Pipeline

End-to-end data pipeline built using Azure Data Factory, ADLS Gen2, Python and PySpark.

## Architecture

CSV Data → Python/Pandas → Azure Data Lake Storage Gen2 → ADF Pipeline → PySpark Analysis → Insights

## Tools & Technologies

- **Azure Data Factory (ADF)** — parameterised pipelines with Metadata and Lookup activities
- **Azure Data Lake Storage Gen2 (ADLS)** — raw and processed data storage
- **Python & Pandas** — data ingestion and initial transformation
- **PySpark & Spark SQL** — distributed data processing and analytics
- **Window Functions** — season-wise player performance ranking

## Pipeline Phases

### Phase 1 — Python/Pandas Pipeline
- Ingests IPL match CSV data
- Filters out DL method matches
- Loads clean data to ADLS Gen2

### Phase 2 — PySpark Pipeline
- Rebuilt pipeline using PySpark for distributed processing
- Implemented Spark SQL with window functions
- Season-wise Orange Cap winner calculation
- Toss impact analysis using aggregations

## Key Findings

- Teams fielding first win **55.9%** of matches vs 45.7% batting first
- **Virat Kohli** scored 973 runs in 2016 — highest single season in dataset
- **CH Gayle** won Player of the Match 21 times — most in IPL history
- **Mumbai Indians** hold the most IPL titles

## SQL Analysis

See `analysis.sql` for full analytical queries including:
- Top 10 run scorers all time
- Top 10 wicket takers
- Orange Cap winners per season
- Toss decision impact on results

## Project Structure

```
ipl-data-pipeline/
├── ipl_pipeline.py        # Main PySpark ETL pipeline
├── ipl_spark.py           # PySpark DataFrame exploration
├── matches.py             # Python/Pandas pipeline
├── load_data.py           # Data loader
├── connection_database.py # Database connection
├── analysis.sql           # SQL analytical queries
└── readme.md              # Project documentation
```

## Dataset

IPL matches dataset sourced from Kaggle.
