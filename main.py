from mylib.extract import extract
from mylib.transform_load import transform
from mylib.query import query

def main():
    """Run the pipeline"""
    print("Step 1: Extracting data...")
    extract()
    
    print("\nStep 2: Transforming data...")
    transform()
    
    print("\nStep 3: Running analysis...")
    query()

if __name__ == "__main__":
    main()