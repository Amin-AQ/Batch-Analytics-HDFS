-- External Table for Raw User Logs
CREATE EXTERNAL TABLE IF NOT EXISTS raw_user_logs (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp STRING,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs/';

-- External Table for Raw Content Metadata
CREATE EXTERNAL TABLE IF NOT EXISTS raw_content_metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata/';

-- Dimension Table for Users
CREATE TABLE IF NOT EXISTS dim_users (
    user_id INT,
    region STRING,
    device STRING
)
STORED AS PARQUET;

-- Dimension Table for Content
CREATE TABLE IF NOT EXISTS dim_content (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
STORED AS PARQUET;

-- Fact Table for User Actions
CREATE TABLE IF NOT EXISTS fact_user_actions (
    user_id INT,
    content_id INT,
    session_id STRING,
    action STRING,
    event_timestamp STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;

