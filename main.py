from mylib.extract import create_spark, load_data
from mylib.transform_load import transform_data
from mylib.query import query

if __name__ == "__main__":
    spark = create_spark("DrugUseAnalysis")
    
    print("Step 1: Extracting data...")
    df = load_data(spark)
    df.show(5)
    
    print("\nStep 2: Transforming data...")
    transformed_df = transform_data(df)
    transformed_df.show(5)
    
    print("\nStep 3: Running analysis query...")
    result = query(transformed_df, spark)
    result.show()
    
    spark.stop()