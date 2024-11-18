"""
Test for drug use data analysis
"""

import os
import pytest
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    """Create a test spark session"""
    spark = start_spark("DrugUseTestApp")
    yield spark
    end_spark(spark)


def test_extract():
    """Test the extract function"""
    try:
        file_path = extract()
        assert os.path.exists(file_path) is True
    except Exception as e:
        print(f"Extract error: {e}")
        assert False


def test_load_data(spark):
    """Test the load data function"""
    try:
        # Ensure data file exists
        extract()
        df = load_data(spark)
        assert df is not None
        assert df.count() > 0
    except Exception as e:
        print(f"Load data error: {e}")
        assert False


def test_describe(spark):
    """Test the describe function"""
    try:
        df = load_data(spark)
        result = describe(df)
        assert result is None
    except Exception as e:
        print(f"Describe error: {e}")
        assert False


def test_query(spark):
    """Test the query function"""
    try:
        df = load_data(spark)
        result = query(
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
        assert result is None
    except Exception as e:
        print(f"Query error: {e}")
        assert False


def test_example_transform(spark):
    """Test the transform function"""
    try:
        df = load_data(spark)
        result = example_transform(df)
        assert result is None
    except Exception as e:
        print(f"Example transform error: {e}")
        assert False


if __name__ == "__main__":
    # Manual testing
    test_spark = start_spark("DrugUseTestApp")
    try:
        test_extract()
        test_load_data(test_spark)
        test_describe(test_spark)
        test_query(test_spark)
        test_example_transform(test_spark)
        print("All tests passed!")
    finally:
        end_spark(test_spark)
