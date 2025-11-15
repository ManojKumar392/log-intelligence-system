# AI-Powered Log Intelligence System

A cloud-native backend + ML system for log ingestion, distributed processing, anomaly detection, and real-time alerting.

This project simulates an enterprise-grade log observability platform with a scalable pipeline, machine learning models, and a dashboard for insights.

---

## 🚀 Features

### 1. Log Ingestion Service
- REST API for receiving logs
- Supports JSON and raw text logs
- Batch ingestion support
- Publishes incoming logs to Kafka

### 2. Distributed Processing Pipeline
- Kafka for streaming logs between services
- Normalizes, filters, and enriches logs
- Stores raw log files in S3/MinIO

### 3. Storage Layer
- PostgreSQL for metadata, anomalies, and alerts
- S3 or MinIO for raw log storage

### 4. Machine Learning Anomaly Detection
- Isolation Forest / LSTM for detecting anomalies
- Detects error bursts, latency spikes, unusual sequences
- Outputs severity + confidence score

### 5. Real-Time Alert Engine
- Listens to anomaly events
- Sends alerts via email, Webhooks, or WebSocket
- Stores alert history in DB

### 6. Dashboard (React + Tailwind)
- Log explorer
- Anomaly timeline
- Error categories
- Real-time alert feed

### 7. Cloud-Native Deployment
- Docker-first development
- Kubernetes-ready deployment
- GitHub Actions CI/CD

---