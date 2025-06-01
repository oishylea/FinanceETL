# 📊 Finance Data ETL Pipeline
A simple ETL (Extract, Transform, Load) pipeline built using **Apache Airflow** and **Docker** to process and categorize finance survey data collected during the COVID-19 lockdown via Google Forms.

---

## 🧠 About the Dataset

- **Source**: [Kaggle - Finance Data](https://www.kaggle.com/datasets/nitindatta/finance-data)
- **Description**: Survey data collected through Google Forms during the COVID-19 lockdown.
- **Example columns**:
  - `GENDER`, `AGE`
  - Investment preferences: Mutual Funds, Equity Market, Government Bonds, PPF, etc.
  - Behavioral traits: Frequency of monitoring, reasons for investing, return expectations, etc.

---

## ⚙️ Pipeline Overview

This project includes two Airflow DAGs:

### 1. `finance_etl_pipeline_ppf`
- Filters the dataset based on the **Public Provident Fund (PPF)** column.
- Saves each PPF preference group into separate CSV files.

### 2. `finance_etl_pipeline_gender`
- Filters the dataset based on **gender**.
- Saves each gender group into separate CSV files.

Each DAG performs:

- **Extract**: Reads the original CSV file.
- **Transform**: Filters based on column values.
- **Load**: Saves the output to separate CSV files in `transformed_data/` and `transformed_data_gender/`.

---


