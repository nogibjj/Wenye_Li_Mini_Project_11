from pyspark.sql import SparkSession

def create_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def query():
    """Query young adult drug use patterns"""
    spark = create_spark("Drug Use Analysis")
    
    df = spark.read.format("delta").load("/dbfs/FileStore/transformed_drug_data")
    df.createOrReplaceTempView("drug_use")
    
    result = spark.sql("""
    SELECT 
        age,
        ROUND(alcohol_use, 2) as alcohol_usage,
        ROUND(marijuana_use, 2) as marijuana_usage,
        ROUND(cocaine_use, 2) as cocaine_usage,
        ROUND(heroin_use, 2) as heroin_usage
    FROM drug_use
    WHERE age IN ('18', '19', '20', '21', '22-23', '24-25')
    ORDER BY alcohol_use DESC
    """)
    
    result.show()
    print("Query completed!")