from pyspark.sql import SparkSession

def create_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract():
    """Extract data and save as delta format"""
    spark = create_spark("DrugUseAnalysis")
    
    # Read CSV and save as delta
    df = spark.read.csv("drug-use-by-age.csv", header=True, inferSchema=True)
    df.write.format("delta").mode("overwrite").save("/dbfs/FileStore/drug_use_data")
    print("Data extraction completed!")