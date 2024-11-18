from pyspark.sql import SparkSession

def create_spark(app_name):
    """
    Creates and returns a Spark session
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def query(df, spark):
    """
    Performs analysis query on drug use among young adults
    """
    df.createOrReplaceTempView("drug_use")
    
    # Query: Most commonly used substances among young adults (18-25)
    sql_query = """
    SELECT 
        age,
        ROUND(alcohol_use, 2) as alcohol_usage,
        ROUND(marijuana_use, 2) as marijuana_usage,
        ROUND(cocaine_use, 2) as cocaine_usage,
        ROUND(heroin_use, 2) as heroin_usage
    FROM drug_use
    WHERE age IN ('18', '19', '20', '21', '22-23', '24-25')
    ORDER BY alcohol_use DESC
    """
    
    result = spark.sql(sql_query)
    return result

def query_data():
    """
    Main function to execute query and display results
    """
    spark = create_spark("Query Drug Use Data")
    
    df = spark.read.format("delta").load("/dbfs/tmp/transformed_drug_data")
    
    result = query(df, spark)
    result.show()
    
    print("Query step completed: Results displayed.")