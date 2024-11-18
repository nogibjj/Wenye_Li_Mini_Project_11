"""Test Databricks functionality: Verify data pipeline"""
from mylib.extract import create_spark, extract
from mylib.transform_load import transform
from mylib.query import query

def test_pipeline():
    """Test the data pipeline functions"""
    # Test extract
    print("Testing data extraction...")
    try:
        df = extract()
        print("✓ Extract test passed")
    except Exception as e:
        print(f"✗ Extract test failed: {e}")
    
    # Test transform
    print("\nTesting data transformation...")
    try:
        transformed_df = transform(df)
        print("✓ Transform test passed")
    except Exception as e:
        print(f"✗ Transform test failed: {e}")
    
    # Test query
    print("\nTesting data query...")
    try:
        query(transformed_df)
        print("✓ Query test passed")
    except Exception as e:
        print(f"✗ Query test failed: {e}")

if __name__ == "__main__":
    test_pipeline()