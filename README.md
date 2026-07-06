🚀 Distributed Log Analytics Using Apache Spark
Overview
This project demonstrates a scalable distributed log analytics pipeline built with Apache Spark on Google Cloud Platform to process large-scale application logs. The pipeline analyzes millions of log records to monitor service performance, detect anomalies, evaluate deployment impact, and demonstrate Spark's fault tolerance capabilities.
The project was developed as part of the Distributed Software Systems course at Concordia University and focuses on applying distributed computing principles to solve real-world observability and analytics challenges.

🎯 Business Problem
Modern applications generate millions of log records every day.
Traditional single-machine processing struggles with this volume, making it difficult to quickly identify:
Performance bottlenecks
Service failures
Latency spikes
Deployment regressions
Infrastructure issues
This project builds a distributed analytics framework capable of processing large log datasets efficiently using Apache Spark while producing actionable insights through interactive dashboards.


🛠 Technologies
Apache Spark
PySpark / Spark SQL
Scala
Google Cloud Platform
Google Cloud Storage (GCS)
Google Cloud Dataproc
BigQuery
Looker Studio
Git
GitHub Actions


🏗 System Architecture
The pipeline follows a distributed architecture consisting of three layers:
Data Ingestion
Reads log datasets from Google Cloud Storage
Loads data into Spark DataFrames
Performs distributed partitioning
Distributed Processing
Executed on a Dataproc Spark Cluster
Data Cleaning
Session Reconstruction
Trace Analysis
Log Aggregation
Window Functions
Anomaly Detection
Deployment Attribution
Fault Tolerance Testing
Analytics & Visualization
Processed results are exported to
BigQuery
Looker Studio
for dashboard visualization.


✨ Key Features
Distributed log processing using Apache Spark
ETL pipeline for large datasets
Sessionization using Window Functions
Trace reconstruction
Service Level Objective (SLO) metrics
p50 / p95 / p99 latency analysis
Rolling window anomaly detection
Deployment impact analysis
Spark fault tolerance validation
BigQuery integration
Interactive dashboards


📊 Analytics Performed
The pipeline calculates
Request Count
Average Latency
p50 Latency
p95 Latency
p99 Latency
Error Rate
Service Availability
Regional Performance
Deployment Regression
Anomaly Severity


📈 Results
The project successfully demonstrated
Scalable distributed log processing
Efficient ETL workflows
Spark SQL analytics
Real-time service performance monitoring
Automated anomaly detection
Deployment impact analysis
Recovery from worker-node failures using Spark lineage
Interactive cloud dashboards for business reporting


👩‍💻 My Contribution
My responsibilities included:
Designing and implementing the Log Analytics pipeline
Building distributed ETL workflows using Apache Spark
Implementing latency and error-rate analysis
Developing rolling-window anomaly detection
Performing deployment attribution analysis
Validating Spark fault tolerance through worker-node failure experiments
Integrating analysis into the final project report


📚 Skills Demonstrated
Distributed Computing
Apache Spark
PySpark
Spark SQL
ETL Pipelines
BigQuery
Google Cloud Platform
Dataproc
Data Engineering
Data Analytics
Window Functions
Fault Tolerance
Dashboard Development
Data Visualization


⭐ Future Improvements
Real-time streaming using Spark Structured Streaming
Kafka integration
Airflow orchestration
Docker deployment
CI/CD pipeline enhancements
Machine learning-based anomaly detection
