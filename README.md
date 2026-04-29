# 🌌 Multi-Pipeline ETL & Reporting Framework
### *Unified Big Data Analytics for Web Server Logs*

![Hadoop MongoDB ETL Banner](./hadoop_mongodb_etl_banner_1777292768623.png)

---

## 📖 Project Overview
This project implements a robust **ETL (Extract, Transform, Load) and Reporting Framework** designed to process large-scale NASA HTTP logs. It features a pluggable architecture allowing seamless switching between **Hadoop MapReduce**, **MongoDB**, **Hive**, and **Pig**, while centralizing all results in a PostgreSQL database.

---

## 🖥 Modern Orchestration (The Frontend)
We have modernized the project with a **Premium Glassmorphic Web Dashboard** that allows for headless pipeline management.

### 🎧 Terminal Listener Architecture
To ensure a 100% environment match with your local Hadoop configuration, the system uses a **Listener-Bridge** pattern:
1. **Frontend (`Node.js`)**: Provides the interactive UI for selecting datasets and engines.
2. **Bridge (`.pipeline_queue`)**: The UI communicates with your terminal via an asynchronous file-based queue.
3. **Listener (`bash listener.sh`)**: A dedicated worker running in your terminal that picks up triggers and executes the Java/Hadoop code natively.

---

## 📊 Advanced Analytics Suite
The `Project/analysis` module provides a comprehensive suite of Java-based visualization tools using **JFreeChart**. These are automatically triggered via the **Master Analyzer**.

| Analyzer | Visualization | Description |
| :--- | :--- | :--- |
| **Data Quality Analyzer** | 🥧 Pie Chart | Breaks down total records into **Valid** vs. **Malformed** segments to audit parsing accuracy. |
| **Pipeline Comparison** | 📊 Bar Chart | Compares the total runtime of different processing engines (e.g., MapReduce vs. MongoDB) on the same dataset. |
| **Query Result Analyzer** | 📈 Statistical Graphs | Visualizes the output of the analytical queries, such as traffic spikes and status code distribution. |
| **Batch Execution Analyzer**| ⏱️ Line/Step Chart | Tracks performance consistency across individual batches during a single run. |
| **Run Metadata Analyzer** | 📋 Historical Trends | Aggregates and compares performance metrics across multiple execution sessions. |

---

## 🚀 Quick Start Guide
To run the full automated system, you need two terminals open:

### 1. Start the Web Server
```bash
cd Project/frontend
node server.js
```

### 2. Start the Terminal Listener
In your interactive terminal (the one where `hadoop` and `mvn` work):
```bash
bash listener.sh
```

### 3. Open the UI
Navigate to `http://localhost:3000` to start processing logs and viewing charts!

---

## 🔍 Parsing Strategy & Integrity
The framework utilizes a high-performance **Master Regular Expression** to extract 9 distinct fields from the Combined Log Format:

```regex
^(\S+)\s+\S+\s+\S+\s+\[(\d{2}/\w{3}/\d{4}):(\d{2}):\d{2}:\d{2}\s+[+-]\d{4}\]\s+"([A-Z]+)\s+(\S+)\s+(HTTP/[\d.]+)"\s+(\d{3})\s+(\S+)$
```
- ✅ **Data Cleaning**: Automatically handles `-` characters in byte fields, converting them to `0`.
- 🚨 **Malformed Tracking**: Records failing to match the regex are counted and reported as "Malformed" in `run_metadata`.

---

<p align="center">
  <i>Developed with ❤️ for the Big Data & NoSQL Community</i>
</p>
