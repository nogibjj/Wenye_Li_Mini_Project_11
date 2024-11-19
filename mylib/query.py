from pyspark.sql import SparkSession

DATABASE_NAME = "drug_analysis_db"

def create_spark(app_name):
    """Create or get Spark session"""
    return SparkSession.builder.appName(app_name).getOrCreate()

def query():
    """Query young adult drug use patterns"""
    spark = create_spark("Drug Use Analysis")
    
    df = spark.table(f"{DATABASE_NAME}.drug_use")
    df.createOrReplaceTempView("drug_use")
    
    # Analyze young adult drug usage
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
    
    print("\nYoung Adult Drug Usage:")
    result.show()
    
    return result

if __name__ == "__main__":
    query()  # Step 3: Query the transformed data
