#!/bin/bash
################################################################################
# Sqoop Import Script - Import All CSV Files to HDFS
# Purpose: Automate data ingestion from local CSV files to Hadoop HDFS
# Usage: bash sqoop_import_all.sh
################################################################################

# Configuration
LOCAL_DATA_DIR="C:/UT Dallas/Class/2025 Fall/BUAN 6346.001 - Big Data/Project"  # UPDATE THIS PATH
HDFS_BASE_DIR="/user/hive/warehouse/fleet_data"
HADOOP_USER="hadoop"  # UPDATE if different

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Fleet Data Import to HDFS${NC}"
echo -e "${BLUE}========================================${NC}"

# Create HDFS directories
echo -e "\n${GREEN}[1/9] Creating HDFS directory structure...${NC}"
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/trucks
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/driver_risk
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/driver_mpg
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/model_risk
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/city_risk
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/geolocation
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/event_type_counts
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/trucks_mg

# Import trucks.csv
echo -e "\n${GREEN}[2/9] Importing trucks.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/trucks.csv" ${HDFS_BASE_DIR}/trucks/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ trucks.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import trucks.csv${NC}"
    exit 1
fi

# Import driver_risk.csv
echo -e "\n${GREEN}[3/9] Importing driver_risk.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/driver_risk.csv" ${HDFS_BASE_DIR}/driver_risk/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ driver_risk.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import driver_risk.csv${NC}"
    exit 1
fi

# Import driver_mpg.csv
echo -e "\n${GREEN}[4/9] Importing driver_mpg.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/driver_mpg.csv" ${HDFS_BASE_DIR}/driver_mpg/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ driver_mpg.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import driver_mpg.csv${NC}"
    exit 1
fi

# Import model_risk.csv
echo -e "\n${GREEN}[5/9] Importing model_risk.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/model_risk.csv" ${HDFS_BASE_DIR}/model_risk/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ model_risk.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import model_risk.csv${NC}"
    exit 1
fi

# Import city_risk.csv
echo -e "\n${GREEN}[6/9] Importing city_risk.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/city_risk.csv" ${HDFS_BASE_DIR}/city_risk/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ city_risk.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import city_risk.csv${NC}"
    exit 1
fi

# Import geolocation.csv
echo -e "\n${GREEN}[7/9] Importing geolocation.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/geolocation.csv" ${HDFS_BASE_DIR}/geolocation/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ geolocation.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import geolocation.csv${NC}"
    exit 1
fi

# Import event_type_counts.csv
echo -e "\n${GREEN}[8/9] Importing event_type_counts.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/event_type_counts.csv" ${HDFS_BASE_DIR}/event_type_counts/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ event_type_counts.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import event_type_counts.csv${NC}"
    exit 1
fi

# Import trucks_mg.csv
echo -e "\n${GREEN}[9/9] Importing trucks_mg.csv...${NC}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/trucks_mg.csv" ${HDFS_BASE_DIR}/trucks_mg/
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ trucks_mg.csv imported successfully${NC}"
else
    echo -e "${RED}✗ Failed to import trucks_mg.csv${NC}"
    exit 1
fi

# Verification
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Verification${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "\n${GREEN}Checking HDFS directory structure:${NC}"
hdfs dfs -ls ${HDFS_BASE_DIR}

echo -e "\n${GREEN}File sizes:${NC}"
hdfs dfs -du -h ${HDFS_BASE_DIR}

echo -e "\n${GREEN}Row counts (first 10 lines of each file):${NC}"
echo "trucks.csv:"
hdfs dfs -cat ${HDFS_BASE_DIR}/trucks/trucks.csv | head -2

echo -e "\ngeolocation.csv:"
hdfs dfs -cat ${HDFS_BASE_DIR}/geolocation/geolocation.csv | head -2

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✓ All files imported successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nNext step: Run Hive table creation script"
echo -e "Command: hive -f hive_create_tables.hql\n"
