Trade Surveillance System 📈🚨
A real-time and batch trade surveillance pipeline built to detect market manipulation, abnormal trading behavior, volume spikes, and price deviations. 
The architecture leverages Kafka for stream ingestion, PySpark for scalable data processing & risk detection, DuckDB for analytical storage, 
dbt for transformations & reporting data marts, and Apache Airflow for orchestration.

🏗 System Architecture & Workflow
Trade Generator -> Kafka Topic -> PySpark Streaming (Risk Detection & Aggregations) -> DuckDB (Raw & Cleaned Trades) 
-> dbt (Staging ➔ Intermediate ➔ Marts) -> Airflow Dag Orchestrator

📁 Repository Structure

├── config/                  # Kafka, PySpark, and environment configurations
├── dbt/                     # dbt transformation models and tests
│   └── trade_surveillance/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           ├── staging/      # Raw data cleanup & source declarations
│           ├── intermediate/ # Trader-level feature aggregations
│           └── marts/        # Compliance & Risk reporting tables
├── dags/                    # Apache Airflow DAGs for pipeline orchestration
├── data/                    # Local storage (trades.db DuckDB database)
├── logs/                    # Pipeline & Spark logging directory
├── docker-compose.yml       # Containerized environment setup
├── requirements.txt         # Python dependencies
└── README.md



🛠 Tech Stack Used :
Streaming / Ingestion: Apache Kafka, Python (kafka-python-ng)
Processing: PySpark (Spark Streaming & Batch)
Storage: DuckDB
Data Transformation: dbt (dbt-duckdb)
Orchestration: Apache Airflow
Containerization: Docker & Docker Compose
