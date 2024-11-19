from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

TEMP_PATH = "/tmp/drug-use-by-age.csv"
DATABASE_NAME = "drug_analysis_db"

def create_spark(app_name):
    """Create or get Spark session"""
    return SparkSession.builder.appName(app_name).getOrCreate()

def transform_and_load():
    """Transforms the data and loads it into Delta table."""
    
    try:
        spark = create_spark("Drug Use Transform")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        
        # Read CSV file
        df = spark.read.csv(f"dbfs:{TEMP_PATH}", header=True, inferSchema=True)
        
        # Transform data
        for column in [c for c in df.columns if c not in ['age', 'n']]:
            df = df.withColumn(column,
                             when(col(column) == "-", None)
                             .otherwise(col(column))
                             .cast("float"))
        
        # Save as Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE_NAME}.drug_use")
        
        print("Data transformation and loading completed successfully!")
        return df
        
    except Exception as e:
        print(f"Error during transformation and loading: {str(e)}")
        return None

if __name__ == "__main__":
    transform_and_load()  # Step 2: Transform and load the data
    