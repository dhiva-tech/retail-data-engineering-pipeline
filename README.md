# Retail Data Engineering Pipeline 🚀

## Project Overview
This project is an end-to-end Retail Data Engineering Pipeline built using PySpark and Delta Lake.

The pipeline follows Medallion Architecture:
- Bronze Layer
- Silver Layer
- Gold Layer

The project also includes:
- Incremental Loading
- SCD Type 2 Tracking
- Star Schema Modeling
- Delta Lake Storage

---

# Architecture

Raw CSV Data
    ↓
Bronze Layer (Raw Ingestion)
    ↓
Incremental Merge
    ↓
Silver Layer (Cleaned & Transformed Data)
    ↓
SCD Type 2 Tracking
    ↓
Gold Layer (Fact & Dimension Tables)


![alt text](architecture.png)

---

# Technologies Used

- Python
- PySpark
- Delta Lake
- Spark SQL
- Git & GitHub
- Linux / WSL

---

# Project Features

## Bronze Layer
- Raw data ingestion
- Metadata columns
- Delta format storage

## Incremental Loading
- MERGE INTO using Delta Lake
- Duplicate handling

## Silver Layer
- Data cleaning
- Data standardization
- Derived columns
- Business transformations

## SCD Type 2
- Historical tracking
- Active/inactive records
- Start and end date handling

## Gold Layer
- Fact table creation
- Dimension table creation
- Star schema modeling

---

# Folder Structure

```bash
data_engineering_project/
│
├── data/
├── delta/
├── spark_jobs/
│   ├── bronze_ingestion.py
│   ├── bronze_incremental_merge.py
│   ├── silver_transformation.py
│   ├── scd2_tracker.py
│   ├── gold_star_schema.py
│
├── main.py
├── requirements.txt
├── README.md
```

---

# How to Run

## Create Virtual Environment

```bash
python3 -m venv venv
```

## Activate Virtual Environment

```bash
source venv/bin/activate
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Run Pipeline

```bash
python3 main.py
```

---

# Sample Outputs

- Bronze Layer Output
- Silver Layer Output
- SCD Type 2 History Table
- Fact Table
- Dimension Tables

---

# Author

Dhivakar M  
B.Tech Information Technology  
Aspiring Data Engineer 