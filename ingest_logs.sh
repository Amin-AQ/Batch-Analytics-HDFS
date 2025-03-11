#!/bin/bash

# Check if a date parameter is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <YYYY-MM-DD>"
    exit 1
fi

# Extract year, month, and day
DATE="$1"
YEAR=$(date -d "$DATE" '+%Y')
MONTH=$(date -d "$DATE" '+%m')
DAY=$(date -d "$DATE" '+%d')

# Local file paths
LOCAL_LOGS_PATH="./raw_data/${DATE}/${DATE}.csv"
LOCAL_METADATA_PATH="./raw_data/content_metadata.csv"

# HDFS target paths
HDFS_LOGS_PATH="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_PATH="/raw/metadata/"   # Year Month Day partitioning would cause redundancy, since metadata contains same content

# Uncomment if dfs and yarn not running
#start-dfs.sh
#start-yarn.sh

# Create HDFS directories
hdfs dfs -mkdir -p "$HDFS_LOGS_PATH"
hdfs dfs -mkdir -p "$HDFS_METADATA_PATH"

# Copy data files into HDFS
hdfs dfs -put -f "$LOCAL_LOGS_PATH" "$HDFS_LOGS_PATH/"
hdfs dfs -put -f "$LOCAL_METADATA_PATH" "$HDFS_METADATA_PATH/"

echo "Data for $DATE successfully ingested into HDFS."

