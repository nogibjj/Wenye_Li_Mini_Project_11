"""
Main CLI or app entry point for drug use data analysis
"""

from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


def main():
    """Main function for drug use data analysis"""

    # Step 1: Extract data from source
    extract(
        url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/drug-use-by-age/drug-use-by-age.csv",
        file_path="data/drug_use_by_age.csv",
    )

    # Step 2: Start Spark session
    spark = start_spark("Drug Use Analysis")

    # Step 3: Load data into DataFrame
    df = load_data(spark, data="data/drug_use_by_age.csv")

    # Step 4: Get basic statistics
    describe(df)

    # Step 5: Execute example SQL query
    query(
        spark,
        df,
        """
        SELECT age, 
               AVG(alcohol_use) as avg_alcohol_use, 
               AVG(marijuana_use) as avg_marijuana_use
        FROM drug_use
        GROUP BY age
        ORDER BY avg_alcohol_use DESC
        """,
        "drug_use",
    )

    # Step 6: Perform example transformation
    example_transform(df)

    # Step 7: End Spark session
    end_spark(spark)


if __name__ == "__main__":
    main()
