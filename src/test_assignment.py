import pytest
from pyspark.sql import SparkSession
from functions import load_csv_in_spark
from functions import filter_column_by_list


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
loading_cases = [
    (
        'src/input_data/dataset_one.csv', 
        1000, 
        ['id', 'first_name', 'last_name', 'email', 'country']
    ),
    (
        'src/input_data/dataset_two.csv', 
        1000, 
        ['id', 'btc_a', 'cc_t', 'cc_n']
    )
]

@pytest.mark.parametrize("file_path,row_count,column_names", loading_cases)
def test_data_loading(spark_session, file_path, row_count, column_names):
    """Tests for the correct properties of both datasets"""
    dataset = load_csv_in_spark(
        spark_session, file_path=file_path
    )

    # Test if loading worked
    assert dataset is not None, "Loading dataset failed."

    # Test if number of rows matches expected
    assert dataset.count() == row_count, "Length of dataset is incorrect."
    
    # Test if the column names match the expected input
    assert sorted(dataset.columns) == sorted(column_names),\
        "Dataset columns not as expected."
     
def test_wrong_file_path(spark_session):
    # Test if a false file path throws an error
    with pytest.raises(AssertionError):
        false_datapath = 'src/input_data/dataset_none.csv'
        load_csv_in_spark(
            spark=spark_session, 
            file_path=false_datapath
        )
        
        
## Data Filtering Tests ##
test_cases = [
    (['Netherlands'], ['Netherlands']),
    (['Netherlands', 'United Kingdom'], ['Netherlands', 'United Kingdom']),
    ([None], []),
    (['Verweggistan'], [])
]

@pytest.mark.parametrize("selection, expectation", test_cases) 
def test_different_filters(spark_session, selection, expectation):
    dataset = load_csv_in_spark(
        spark=spark_session,
        file_path='src/input_data/dataset_one.csv'
    ) 

    filtered_df = filter_column_by_list(
        dataframe=dataset,
        column_name='country',
        filter_list=selection
    )
    unique_values = (filtered_df
                     .dropDuplicates(['country'])
                     .select('country')
                     .collect()
    )
    unique_countries = [row.country for row in unique_values]

    assert sorted(unique_countries) == expectation,\
        f"Filter did not work: {selection}"
    
