"""Test Databricks functionality: Verify DBFS paths for drug use data"""
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

# Define paths to test
PATHS_TO_TEST = [
    "dbfs:/FileStore/drug_use_data",         # Raw data
    "dbfs:/FileStore/transformed_drug_data"   # Transformed data
]

url = f"https://{server_h}/api/2.0"

def check_data_path(path, headers):
    """Check if path exists in DBFS"""
    try:
        response = requests.get(
            url + f"/dbfs/get-status?path={path}", 
            headers=headers
        )
        response.raise_for_status()
        return response.json()["path"] is not None
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False

def test_databricks():
    """Test if the specified paths exist in DBFS"""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    for path in PATHS_TO_TEST:
        exists = check_data_path(path, headers)
        if exists:
            print(f"✓ Path exists: {path}")
        else:
            print(f"✗ Path does not exist: {path}")

if __name__ == "__main__":
    test_databricks()