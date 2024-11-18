[![CI](https://github.com/nogibjj/Wenye_Li_Mini_Project_10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Wenye_Li_Mini_Project_10/actions/workflows/cicd.yml)
# Wenye Li Mini Project 10

## Project Description
This project utilizes PySpark to process and analyze drug use data. The focus is on extracting insights from age-specific drug usage patterns, performing data transformations, and leveraging Spark SQL for complex queries.

## File Structure
- `main.py`: Entry point for running the analysis.
- `mylib/lib.py`: Contains reusable functions for data processing.
- `test.py`: Unit tests for the functions in `lib.py`.

## Key Features

- Data extraction and preprocessing
- Basic descriptive statistical analysis
- Grouping and aggregation using Spark SQL
- Categorization and transformation of data to highlight alcohol risk levels
- Automated CI/CD pipeline for testing and formatting

## How to Run
1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```
2. Install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Run the main script:
   ```bash
   python main.py
   ```
4. Run tests:
   ```bash
   pytest
   ```

## Data Processing Flow

- Extract: Download the dataset using the extract() function.
- Initialize: Start a Spark session with start_spark().
- Load: Load the dataset into a PySpark DataFrame using load_data().
- Analyze: Generate descriptive statistics with describe().
- Query: Use Spark SQL in the query() function to group and aggregate data by age and substance usage.
- Transform: Apply data transformations with example_transform() to categorize alcohol usage into risk levels.
- Cleanup: End the Spark session with end_spark().