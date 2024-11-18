from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def create_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def transform():
    """Transform data and save"""
    spark = create_spark("Transform Drug Use Data")
    df = spark.read.format("delta").load("/dbfs/FileStore/drug_use_data")
    
    # Transform numeric columns
    for column in [c for c in df.columns if c not in ['age', 'n']]:
        df = df.withColumn(column,
                          when(col(column) == "-", None)
                          .otherwise(col(column))
                          .cast("float"))
    
    # Transform 'n' column
    df = df.withColumn("n", col("n").cast("int"))
    
    # Save transformed data
    df.write.format("delta").mode("overwrite").save("/dbfs/FileStore/transformed_drug_data")
    print("Transform step completed!")