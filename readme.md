# IPL Data Pipeline

End to end data pipeline built on Azure and Python.

## What it does
- Ingests IPL match data from CSV
- Filters matches where DL method was applied
- Saves clean output for analysis

## Tools used
- Python, Pandas
- Azure Data Factory
- Azure Data Lake Storage Gen2

PySpark Pipeline (Phase 2)

Rebuilt the pipeline using PySpark for distributed data processing.

## Tech added
- PySpark 4.1.1
- Spark SQL
- Window functions

## New files
- `ipl_spark.py` — PySpark DataFrame exploration
- `ipl_pipeline.py` — Full ETL pipeline in PySpark

## Analyses
- Most IPL titles per team
- Top 10 run scorers all time
- Top 10 wicket takers
- Orange cap winner per season (Window function)
- Toss impact on match result (Spark SQL)

## Key findings
- Teams fielding after toss win win 55.9% of matches vs 45.7% batting first
- Virat Kohli scored 973 runs in 2016 — highest single season in dataset
- CH Gayle won Player of the Match 21 times — most in IPL history

## Dataset
IPL matches dataset from Kaggle
