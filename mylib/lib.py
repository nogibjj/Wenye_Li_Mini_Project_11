"""
Library functions for drug use data analysis
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

LOG_FILE = "drug_use_pyspark_output.md"


def log_output(operation, output, query=None):
    """Log operation results to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"## Operation: {operation}\n\n")
        if query:
            file.write(f"### Query: \n{query}\n\n")
        file.write(f"### Output: \n\n{output}\n\n")


def start_spark(app_name):
    """Start a Spark session"""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def end_spark(spark):
    """Stop the Spark session"""
    spark.stop()
    return "Stopped Spark session"


def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/drug-use-by-age/drug-use-by-age.csv",
    file_path="data/drug_use_by_age.csv",
    directory="data",
):
    """Download dataset from a URL to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as response:
        with open(file_path, "wb") as file:
            file.write(response.content)
    return file_path


def load_data(spark, data="data/drug_use_by_age.csv", name="drug_use"):
    """Load drug use data with schema"""
    schema = StructType(
        [
            StructField("age", StringType(), True),
            StructField("n", FloatType(), True),
            StructField("alcohol_use", FloatType(), True),
            StructField("alcohol_frequency", FloatType(), True),
            StructField("marijuana_use", FloatType(), True),
            StructField("marijuana_frequency", FloatType(), True),
            StructField("cocaine_use", FloatType(), True),
            StructField("cocaine_frequency", FloatType(), True),
            StructField("crack_use", FloatType(), True),
            StructField("crack_frequency", FloatType(), True),
            StructField("heroin_use", FloatType(), True),
            StructField("heroin_frequency", FloatType(), True),
            StructField("hallucinogen_use", FloatType(), True),
            StructField("hallucinogen_frequency", FloatType(), True),
            StructField("inhalant_use", FloatType(), True),
            StructField("inhalant_frequency", FloatType(), True),
            StructField("pain_releiver_use", FloatType(), True),
            StructField("pain_releiver_frequency", FloatType(), True),
            StructField("oxycontin_use", FloatType(), True),
            StructField("oxycontin_frequency", FloatType(), True),
            StructField("tranquilizer_use", FloatType(), True),
            StructField("tranquilizer_frequency", FloatType(), True),
            StructField("stimulant_use", FloatType(), True),
            StructField("stimulant_frequency", FloatType(), True),
            StructField("meth_use", FloatType(), True),
            StructField("meth_frequency", FloatType(), True),
            StructField("sedative_use", FloatType(), True),
            StructField("sedative_frequency", FloatType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(data)
    log_output("Load Data", df.limit(10).toPandas().to_markdown())
    return df


def query(spark, df, query, name="drug_use"):
    """Query data using Spark SQL"""
    df.createOrReplaceTempView(name)
    log_output("Query Data", spark.sql(query).toPandas().to_markdown(), query)
    return spark.sql(query).show()


def describe(df):
    """Get basic statistics of the numeric columns"""
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("Describe Data", summary_stats_str)
    return df.describe().show()


def example_transform(df):
    """Transform drug use data to add alcohol risk categories"""
    df = df.withColumn(
        "alcohol_risk",
        when(col("alcohol_use") > 50, "High")
        .when(col("alcohol_use") > 30, "Moderate")
        .otherwise("Low"),
    )

    log_output("Transform Data", df.limit(10).toPandas().to_markdown())
    return df.show()