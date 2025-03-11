-- Load Data into `dim_users`
INSERT OVERWRITE TABLE dim_users
SELECT DISTINCT user_id, region, device FROM raw_user_logs;

-- Load Data into `dim_content`
INSERT OVERWRITE TABLE dim_content
SELECT DISTINCT * FROM raw_content_metadata;

-- Load Data into `fact_user_actions`
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT user_id, content_id, session_id, action, event_timestamp, year, month, day
FROM raw_user_logs;

