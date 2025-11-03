# 🚀 Storage Monitoring & Metadata Pipeline (PySpark + MySQL + Superset)

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)]()
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.x-orange.svg)]()
[![MySQL](https://img.shields.io/badge/Database-MySQL-blue.svg)]()
[![Superset](https://img.shields.io/badge/BI-Apache%20Superset-green.svg)]()
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)]()

A production-grade system that **scans Virtual Machine storage**, extracts detailed file metadata, computes hashes, writes **Parquet backups**, loads the Metadata into **MySQL**, and visualizes insights using **Apache Superset**.

This project is built using:

✅ **PySpark** for distributed metadata scanning  
✅ **MySQL** for centralized storage  
✅ **Superset** for analytics dashboards  
✅ **Bash scripts** for automation  

Ideal for:
- Storage monitoring  
- Duplicate file detection  
- Capacity planning  
- Infrastructure audits  
- Linux engineering teams  
- Enterprise server monitoring  

---

# 📁 Project Structure
```bash
VM-File-Monitoring-Setup/
│
├── storage_monitor/
│ ├── config.yaml           # Scanner configuration
│ ├── requirements.txt      # Python dependencies
│ ├── run_scan.sh           # Shell wrapper
│ ├── scan_vm_to_mysql.py   # MySQL ingestion pipeline
│ └── scripts/
│   ├── helpers.py          # Hashing utilities
│   └── scan.py             # PySpark metadata scanner
│
├── superset/
│ └── superset_config.py    # Superset backend configuration
│
├── .gitignore
└── README.md
```

---

# ✨ Features

### ✅ **1. Distributed File Scanning (PySpark)**
- Scans entire VM storage
- Parallel directory traversal
- Computes metadata such as size, owner, permissions, extension, time etc.
- Depth-based directory chunking for improved performance

### ✅ **2. Parquet Backup Storage**
- Metadata stored in **columnar Parquet format**
- Can be consumed by Spark, Pandas, or Athena

### ✅ **3. MySQL Storage for Dashboards**
- Clean schema designed for BI tools
- Can scale to millions of file records

### ✅ **4. Superset Dashboard**
- View storage usage trends
- Track large files
- Filter by owner, extension, directory, date
- Detect duplicates

### ✅ **5. Fully Configurable**
Change scanning paths, hashing thresholds, workers, log paths from `config.yaml`.

### ✅ **6. Ready for Production**
- Works on any Linux VM
- Supports cron scheduling
- Designed for scaling

---

# 🛠 Prerequisites

✅ Python **3.10+**  
✅ Apache **Spark 4.x** installed  
✅ MySQL server running  
✅ Superset installed & configured  
✅ Linux environment with file system access  

---

# 🚀 Getting Started

## **1. Clone the Repository**
```bash
git clone https://github.com/Durvesh123/VM-File-Monitoring-Setup.git
cd VM-File-Monitoring-Setup/storage_monitor
```
## **2. Create Virtual Environment**
```bash
python3 -m venv venv
source venv/bin/activate
```
## **3. Install Dependencies**
```bash
pip install -r requirements.txt
```
## **4. Configure the Scanner**
Edit config.yaml:
```bash
root_paths:
  - /path/to/scan
max_workers: 4
output_dir: ./outputs/metadata
```
## **5. Run the Metadata Scanner**
```bash
./run_scan.sh
```
This will:
✅ Scan directories
✅ Collect metadata
✅ Hash files
✅ Save Parquet output

## **6. Load Metadata Into MySQL**
Set environment variables before running:
```bash
export MYSQL_USER="root"
export MYSQL_PASS="yourpassword"
```
Then execute:
```bash
python3 scan_vm_to_mysql.py
```
## **7. Visualize in Apache Superset**
Place your Superset configuration:
```bash
superset/superset_config.py
```
Start Superset:
```bash
superset run -p 8088 --with-threads --reload --debugger
```
Connect to MySQL → Add table → Build dashboards.

# 🧠 How It Works (Architecture Overview)
## **1. Scanner Phase (PySpark)**
The scanner:
- Reads directories in parallel
- Performs lightweight metadata extraction
- Avoids heavy operations until necessary
- Writes output to Parquet

## **2. Duplicate Detection Logic**
| Step | Action |
|------|---------|
| 1    | Compare file size |
| 2    | Compute partial hash |
| 3    | Group matching files |
| 4    | Compute full hash only when needed |
| 5    | Duplicate confirmed |

Efficient and scalable.

## **3. MySQL Ingestion**

scan_vm_to_mysql.py loads all metadata into a relational model.
- ✅ Good for BI
- ✅ Easy to query
- ✅ Retains full audit history
