from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def transform_data(df):
    """
    Transforms the drug use data by:
    1. Converting string numbers to float type
    2. Replacing '-' with null values
    3. Standardizing column types
    """
    # Get list of columns except 'age' and 'n'
    numeric_columns = [
    col_name for col_name in df.columns 
    if col_name not in ['age', 'n']
    ]
    
    # Start with original dataframe
    transformed_df = df
    
    # Replace '-' with null and convert to float for numeric columns
    for column in numeric_columns:
        transformed_df = transformed_df.withColumn(
            column,
            col(column).cast("string")  # Ensure string type first
        )
        transformed_df = transformed_df.withColumn(
            column,
            regexp_replace(col(column), "-", None).cast("float")
        )
    
    # Convert 'n' (number of respondents) to integer
    transformed_df = transformed_df.withColumn("n", col("n").cast("int"))
    
    return transformed_df

def transform():
    spark = create_spark("Transform Drug Use Data")
    
    # Read the extracted data
    df = spark.read.format("delta").load("/dbfs/tmp/drug_use_data")
    
    # Transform the data
    transformed_df = transform_data(df)
    
    # Save transformed data
    transformed_df.write.format("delta").save("/dbfs/tmp/transformed_drug_data")
    print("Transform step completed: Data saved.")