from pyspark.sql import SparkSession

def create_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract():
    """Extract data from CSV"""
    spark = create_spark("DrugUseAnalysis")
    
    # Read CSV file
    df = spark.read.csv("drug-use-by-age.csv", header=True, inferSchema=True)
    return df