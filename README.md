# 🚦 Urban Traffic Crash Risk Analytics Pipeline

An end-to-end Data Engineering project that collects, processes, transforms, and analyzes urban traffic crash data using a modern data pipeline architecture. The project follows the Bronze–Silver–Gold architecture and includes workflow orchestration with Apache Airflow and an interactive dashboard using Streamlit.

---

## 📌 Project Overview

This project automates the complete data engineering workflow for urban traffic crash analysis.

The pipeline:

- Collects raw traffic crash data
- Stores raw data in the Bronze layer
- Cleans and transforms data into the Silver layer
- Creates business-ready datasets in the Gold layer
- Builds a Star Schema warehouse
- Implements Slowly Changing Dimension (SCD Type 2)
- Automates execution using Apache Airflow
- Visualizes insights using Streamlit

---

## 🏗️ Architecture

```
Traffic Crash API
        │
        ▼
 Bronze Layer (Raw Data)
        │
        ▼
 Silver Layer (Cleaned Data)
        │
        ▼
 Gold Layer (Business Aggregates)
        │
        ▼
 Data Warehouse (Star Schema)
        │
        ▼
 Streamlit Dashboard
```

---

## 📂 Project Structure

```
Urban Traffic Crash Risk/
│
├── airflow/
│   └── dags/
│       └── traffic_pipeline_dag.py
│
├── data_ingestion/
│   ├── crash_api_extract.py
│   ├── youtube_api_extract.py
│   ├── spark_jobs/
│   │   ├── cleaning_jobs.py
│   │   ├── gold_layer.py
│   │   ├── star_schema.py
│   │   └── scd_dimension.py
│
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── warehouse/
│
├── dashboard.py
├── Dockerfile
├── docker-compose.yml
├── run.sh
└── README.md
```

---

## 🚀 Features

- End-to-End ETL Pipeline
- Bronze-Silver-Gold Architecture
- PySpark Data Processing
- Incremental Data Loading
- Data Quality Checks
- Star Schema Data Warehouse
- SCD Type 2 Implementation
- Apache Airflow Workflow Automation
- Dockerized Environment
- Interactive Streamlit Dashboard
- Modular Project Structure

---

## 🛠️ Technologies Used

- Python
- PySpark
- Apache Airflow
- Docker
- Streamlit
- Pandas
- Parquet
- SQL
- Git & GitHub

---

## ▶️ How to Run

### Clone Repository

```bash
git clone https://github.com/<your-username>/urban-traffic-crash-risk.git
cd urban-traffic-crash-risk
```

### Start Docker

```bash
docker-compose up --build -d
```

### Run Silver Layer

```bash
./run.sh cleaning_jobs.py
```

### Run Gold Layer

```bash
./run.sh gold_layer.py
```

### Build Warehouse

```bash
./run.sh star_schema.py
```

### Run SCD

```bash
./run.sh scd_dimension.py
```

### Launch Streamlit Dashboard

```bash
streamlit run dashboard.py
```

Open:

```
http://localhost:8501
```

---

## 📊 Dashboard

The Streamlit dashboard provides:

- Monthly Crash Trends
- High-Risk Areas
- Crash Distribution
- Traffic Analytics
- Interactive Charts

---

## 🔄 Airflow Pipeline

The project includes an Apache Airflow DAG that automates:

```
Silver Layer
      ↓
Gold Layer
      ↓
Warehouse
      ↓
SCD Type 2
```

---

## 📈 Future Enhancements

- Cloud Deployment (AWS/GCP/Azure)
- Real-Time Streaming with Kafka
- Delta Lake Integration
- Data Validation using Great Expectations
- Email Alerts
- CI/CD Pipeline
- Monitoring & Logging

---

## 👨‍💻 Author

**Dharshan L**

B.Tech Artificial Intelligence & Data Science

GitHub: https://github.com/<your-username>

---

## 📜 License

This project is developed for educational and portfolio purposes.
