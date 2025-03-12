# Batch-Analytics-HDFS
 Assignment 2 for Data Engineering Course (Group 9)
 
 **Big Data Pipeline: Hive and Hadoop Implementation**

## **1. Introduction**
This document outlines the implementation of a data pipeline using **Hadoop and Hive**, designed to process streaming-like user activity logs and content metadata. The pipeline follows a **star schema** with a central fact table and supporting dimension tables to enable efficient analytical queries.

## **2. Design Choices**
### **Data Storage & Partitioning**
- **Raw Data Storage:** Data is first ingested into **HDFS** in the `/raw/logs/` and `/raw/metadata/` directories.
- **Hive External Tables:** Created for raw logs and metadata to provide flexibility in schema evolution.
- **Partitioning Strategy:**
  - `fact_user_actions` table is **partitioned by (year, month, day)** to improve query performance by reducing scan times.
  - **Dynamic partitioning** is enabled for efficient data ingestion.
- **Columnar Format (Parquet):** Fact and dimension tables are stored in **Parquet format** for better compression and faster queries.

### **Data Ingestion & Transformation**
- **Shell Script (`ingest_logs.sh`)** automates the ingestion of daily log files into HDFS.
- **Hive SQL transformation scripts** move data from raw tables to fact and dimension tables.
- **Indexes are not used** due to Hive’s distributed processing model, which favors partitioning over indexing.

## **3. Performance Optimization Considerations**
### **1. Partition Pruning**
By using **year, month, day** partitions in `fact_user_actions`, queries only scan relevant partitions, significantly improving performance.

### **2. Parallel Processing & Resource Allocation**
- **Hive configurations set:**
  ```sql
  SET hive.exec.dynamic.partition.mode=nonstrict;
  SET hive.exec.dynamic.partition=true;
  ```
  - These settings allow **parallel partition creation** instead of requiring predefined partitions.
- **Resource Management:** YARN manages memory and CPU allocation for query execution.

## **3. Execution Time Analysis**
### **Pipeline Execution Time**
| Stage                         | Execution Time            |
| ----------------------------- | ------------------------- |
| **Data Ingestion (HDFS)**     | ~10 sec                  |
| **Raw Table Creation**        | ~20 sec                   |
| **Data Transformation (ETL)** | ~15 - 35 sec per table             |
| **Query Execution (Hive)**    | ~20 - 40 sec per query |

### **Query Execution Time Analysis**
| Query                          | Execution Time |
| ------------------------------ | -------------- |
| Monthly Active Users by Region | ~20.6 sec     |
| Top Categories by Play Count   | ~38.1 sec     |
| Average Session Count Per Week | ~34.2 sec      |

## **4. Hive Queries Used**
### **Data Transformation Queries**
```sql
-- Load Data into dim_users
INSERT OVERWRITE TABLE dim_users
SELECT DISTINCT user_id, region, device FROM raw_user_logs;

-- Load Data into dim_content
INSERT OVERWRITE TABLE dim_content
SELECT DISTINCT * FROM raw_content_metadata;

-- Load Data into `dim_sessions`
INSERT OVERWRITE TABLE dim_sessions
SELECT DISTINCT session_id, user_id FROM raw_user_logs;

-- Load Data into fact_user_actions (Dynamic Partitioning)
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT user_id, content_id, session_id, action, event_timestamp, year, month, day
FROM raw_user_logs;
```

### **Analytical Queries**
```sql
-- Monthly Active Users by Region
SELECT dim_users.region, COUNT(DISTINCT fact_user_actions.user_id) AS active_users
FROM fact_user_actions
JOIN dim_users ON fact_user_actions.user_id = dim_users.user_id
WHERE fact_user_actions.year = 2023 AND fact_user_actions.month = 9
GROUP BY dim_users.region;

-- Top Categories by Play Count
SELECT dim_content.category, COUNT(*) AS play_count
FROM fact_user_actions
JOIN dim_content ON fact_user_actions.content_id = dim_content.content_id
WHERE fact_user_actions.action = 'play'
GROUP BY dim_content.category
ORDER BY play_count DESC
LIMIT 5;

-- Average Session Count Per Week
SELECT fact_user_actions.year, WEEKOFYEAR(fact_user_actions.event_timestamp) AS week,
       COUNT(DISTINCT fact_user_actions.session_id) AS total_sessions
FROM fact_user_actions
GROUP BY fact_user_actions.year, WEEKOFYEAR(fact_user_actions.event_timestamp)
ORDER BY fact_user_actions.year, week;
```

## **5. Conclusion**
This pipeline successfully demonstrates an efficient **Hadoop + Hive-based ETL process** for processing user activity logs. Performance optimizations such as **partitioning and Parquet storage** have significantly reduced execution time, making the system scalable for large datasets.

Future optimizations may include **bucketing for further query efficiency** and **leveraging Apache Spark** for even faster query execution on large-scale data.



