import pytest
from pyspark.sql import SparkSession
from functions import load_csv_in_spark


@pytest.fixture(scope="module")
def spark_session():
    # Create a spark session for testing
    spark = (SparkSession.builder
            .master("local")
            .appName("TestSession")
            .getOrCreate()
        )
    yield spark
    # Tear down the session after testing
    spark.stop()

## Data Loading tests ##
def test_data_loading(spark_session):
    """Tests for the correct properties of both datasets"""
    dataset_1 = load_csv_in_spark(
        spark_session, 'src/input_data/dataset_one.csv'
    )
    dataset_2 = load_csv_in_spark(
        spark_session, 'src/input_data/dataset_two.csv'
    )
    
    assert dataset_1 is not None, "Loading dataset 1 failed."
    assert dataset_2 is not None, "Loading dataset 2 failed."
    
    assert dataset_1.count() == 1000, "Length of dataset 1 is incorrect."
    assert dataset_2.count() == 1000, "Length of dataset 2 is incorrect."
    
    assert dataset_1.columns == ['id', 'first_name', 'last_name', 'email', 'country'],\
        "Dataset 1 columns not as expected."
    assert dataset_2.columns == ['id', 'btc_a', 'cc_t', 'cc_n'],\
        "Dataset 2 columns not as expected."
