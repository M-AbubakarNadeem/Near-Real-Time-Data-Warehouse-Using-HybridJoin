# Near-Real-Time Data Warehouse Using HYBRIDJOIN

*A near-real-time data warehousing system that continuously enriches streaming retail transactions using the HYBRIDJOIN algorithm to enable fast and meaningful business analytics.*

---

## Badges
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Database](https://img.shields.io/badge/Database-MySQL-blue)

---

## Table of Contents
- [About](#about)
- [Why This Project Matters](#why-this-project-matters)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Installation](#installation)
- [How to Run the Project](#how-to-run-the-project)
- [Project Structure](#project-structure)
- [How HYBRIDJOIN Is Used](#how-hybridjoin-is-used)
- [OLAP & Analytics](#olap--analytics)
- [Author](#author)

---

## About

Modern retail organizations such as Walmart generate **continuous streams of transactional data** from online platforms and physical stores. While this data is valuable, it is **not immediately useful for analysis** because it lacks contextual information such as customer demographics, product details, and store metadata.

This project implements a **Near-Real-Time Data Warehouse** that:
- Continuously ingests transactional data
- Enriches it using customer and product master data
- Loads the enriched data into a star-schema data warehouse
- Enables advanced analytical (OLAP) queries for business intelligence

The system is inspired by the **HYBRIDJOIN algorithm**, which is specifically designed for efficiently joining fast data streams with large disk-based relations.

---

## Why This Project Matters

### The Problem

Traditional ETL systems work in **batch mode**:
- Data is collected over long periods
- Processing happens at scheduled interval
- Analytics are delayed

This delay prevents organizations from reacting quickly to changing business conditions.

### Why HYBRIDJOIN?

- Transactional data arrives as a **stream**
- Master data is **large and disk-resident**
- Loading all master data into memory is inefficien
- Naive joins do not scale for real-time analytics

HYBRIDJOIN solves this by:
- Incrementally processing streaming data
- Efficiently accessing only required master dat
- Producing joined results continuously

This enables **near-real-time decision-making**.

---

## Features

- Near-real-time ETL pipelin
- Continuous processing of transactional data
- Stream–relation join using HYBRIDJOIN principles
- Star-schema data warehouse design
- Advanced OLAP queries
- Multithreaded processing for real-time simulation
- Optimized relational storage for analytics

---

## Tech Stack

### Database
= MySQL 8.0

### Programming Language
- Python 3.x

### Python Libraries
- `pandas` – data manipulation
- `mysql-connector-python` – database connectivity

### ETL Algorithm
- **HYBRIDJOIN**

### Tools & Environment
- MySQL Workbench
- VS Code / Terminal
- Windows Command Prompt

---

## Installation

Clone the Repository
git clone https://github.com/your-username/Near-Real-Time-Data-Warehouse-Using-HybridJoin.git
cd Near-Real-Time-Data-Warehouse-Using-HybridJoin

---

## How to Run the Project
This section explains **step-by-step** how to run the entire project from scratch.

## Step 1: Open Terminal (Windows)

1. Press **Win + R**
2. Type `cmd`
3. Press **Enter**

## Step 2: Navigate to Project Folder

- Navigate to the directory where the project files are located.

Example:
cd C:\Users\YourName\walmart-dw-project

1. Install Dependencies
- pip install -r requirements.txt

2. Create Database Schema
- Run the following SQL script in MySQL:

- Create-DW.sql

3. Load Master Data
- Run the master data loading script:

- python Load-Master-Data.py

- Enter Database Credentials When Prompted
- Enter host:
whatever your host name is

- Enter database name:
[Press Enter]

- Enter username:
username

- Enter password:
[Enter your MySQL password]

- Enter customer CSV file path (default: customer_master_data.csv):
[Press Enter]

- Enter product CSV file path (default: product_master_data.csv):
[Press Enter]

4. Run HYBRIDJOIN Near-Real-Time ETL
After master data is loaded, run the HYBRIDJOIN ETL script:

- python Hybrid-Join.py

- Enter Database Credentials When Prompted
- Enter host:
  whatever your host name is

- Enter database name:
[Press Enter]

- Enter username:
username

- Enter password:
[Enter your MySQL password]

- Enter transactional data CSV file (default: transactional_data.csv):
[Press Enter]

What This Script Does
- Connects to MySQL
- Loads master data into memory
- Reads transactional data as a stream
- Applies the HYBRIDJOIN algorithm
- Enriches transactions with dimension data
- Loads data into fact_sales table in near real-time

5. Run Analytical Queries
- Execute OLAP queries for analysis:
- OLAP-Queries.sql

---

## Project Structure

Near-Real-Time-Data-Warehouse-Using-HybridJoin/
│
├── Create-DW.sql        # Star schema and date dimension
├── OLAP-Queries.sql    # Analytical queries
├── Load-Master-Data.py     # Loads dimension tables
├── Hybrid-Join.py          # Near-real-time ETL using HYBRIDJOIN
├── transactional_data.csv
├── customer_master_data.csv
└── product_master_data.csv
├── requirements.txt
└── README.md

---

##  How HYBRIDJOIN Is Used
HYBRIDJOIN is a stream–relation join algorithm designed for scenarios where:

- One dataset arrives continuously as a stream
- The other dataset is large and disk-based550

---

## Workflow in This Project
1. Transactional data is read in chunks (simulating a stream)
2. Incoming records are buffered in memory
3. Stream tuples are matched against customer and product master data
4. Only valid and complete records are joined
5. Enriched records are batch-inserted into the fact table
6. The warehouse is updated continuously

This project implements a practical adaptation of HYBRIDJOIN, preserving its core ideas while adapting it to Python, CSV-based streams, and relational databases.

---

## OLAP & Analytics
The Data Warehouse supports complex analytical queries, including:
•	Drill-down and roll-up analysis
•	Revenue and growth trends
•	Customer behavior analysi
•	Product and store performance ranking
•	Window-function-based insights
•	View-based BI reporting

These queries convert raw transactional data into actionable business intelligence.

---

Author
Muhammad Abubakar Nadeem
abubakarnadeen01234@gmail.com
