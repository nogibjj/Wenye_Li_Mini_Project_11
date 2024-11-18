[![CI](https://github.com/nogibjj/Wenye_Li_Mini_Project_11/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Wenye_Li_Mini_Project_11/actions/workflows/cicd.yml)
# Wenye Li Mini Project 11

## Overview
This project implements a **Databricks-based data pipeline** for analyzing substance use patterns across different age groups. Using PySpark for data processing, the pipeline follows a structured ETL (Extract, Transform, Load) approach to process and analyze drug use survey data from FiveThirtyEight.

## Key Components
The pipeline consists of three main stages:
1. **Data Extraction**: Fetches drug use survey data from FiveThirtyEight's public repository
2. **Data Transformation**: Standardizes data types and cleans missing values
3. **Analysis**: Performs targeted queries focusing on substance use patterns among young adults

## Pipeline Architecture

### Data Flow
1. **Source**: FiveThirtyEight's drug use dataset 
2. **Processing**: PySpark-based data cleaning and transformation
3. **Storage**: Delta Lake format for efficient data management
4. **Analysis**: SQL queries for demographic analysis

### Implementation Details
- **Extract Stage**: `extract.py`
  - Reads CSV data from FiveThirtyEight
  - Stores raw data in Delta format

- **Transform Stage**: `transform_load.py`
  - Standardizes data types
  - Handles missing values
  - Saves processed data

- **Query Stage**: `query.py`
  - Analyzes substance use patterns
  - Focuses on young adult demographics

## Data Architecture

### Source Configuration
- **Type**: CSV file
- **Location**: FiveThirtyEight GitHub repository
- **Format**: Structured data with age groups and usage statistics

### Storage Configuration
- **Format**: Delta Lake
- **Paths**:
  - Raw data: `/dbfs/tmp/drug_use_data`
  - Processed data: `/dbfs/tmp/transformed_drug_data`