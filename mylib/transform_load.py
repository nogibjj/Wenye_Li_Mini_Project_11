from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def create_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def transform(df):
    """Transform drug use data"""
    # Transform numeric columns
    for column in [c for c in df.columns if c not in ['age', 'n']]:
        df = df.withColumn(column,
                          when(col(column) == "-", None)
                          .otherwise(col(column))
                          .cast("float"))
    
    # Transform 'n' column to integer
    df = df.withColumn("n", col("n").cast("int"))
    print("Data transformation completed!")
    return df