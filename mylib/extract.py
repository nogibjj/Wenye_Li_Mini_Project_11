from pyspark.sql import SparkSession
import requests

SOURCE_URL = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/drug-use-by-age/drug-use-by-age.csv"
TEMP_PATH = "/tmp/drug-use-by-age.csv"

def create_spark(app_name):
    """Create or get Spark session"""
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract():
    """Downloads data from source URL and saves to DBFS."""
    response = requests.get(SOURCE_URL)
    
    if response.status_code == 200:
        # Write content to DBFS
        dbutils.fs.put(TEMP_PATH, response.content.decode(), True)
        print(f"File downloaded successfully to {TEMP_PATH}")
        return True
    else:
        print("Failed to download file")
        return False

if __name__ == "__main__":
    extract()  # Step 1: Extract data
    