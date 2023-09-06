import pytest
from pyspark.sql import SparkSession
from functions import load_csv_in_spark
from functions import filter_column_by_list
from functions import remove_columns
from functions import rename_columns


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
filter_cases = [
    (['Netherlands'], ['Netherlands']),
    (['Netherlands', 'United Kingdom'], ['Netherlands', 'United Kingdom']),
    (['Verweggistan'], []),
    ([None], [])
]

@pytest.mark.parametrize("selection, expectation", filter_cases) 
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
    
    
## Column Removal Tests ##
removal_cases = [
    ('src/input_data/dataset_one.csv', 'first_name', False),
    ('src/input_data/dataset_one.csv', ['first_name','last_name'], False),
    ('src/input_data/dataset_one.csv', 'postal_code', True),
    ('src/input_data/dataset_two.csv', 'cc_n', False),
    ('src/input_data/dataset_two.csv', ['cc_n'], False),
    ('src/input_data/dataset_two.csv', ['credit_card'], True)
]

@pytest.mark.parametrize("file_path,columns_to_remove,expect_to_fail", removal_cases)
def test_column_removal(spark_session, file_path, columns_to_remove, expect_to_fail):
    dataset = load_csv_in_spark(spark_session, file_path)
    
    if not expect_to_fail:
        after_removal = sorted([
            col for col in dataset.columns 
            if col not in columns_to_remove
        ])
        dataset_removed = remove_columns(dataset, columns_to_remove)
        
        assert sorted(dataset_removed.columns) == after_removal,\
            "Column removal not successful"
    
    elif expect_to_fail:
        with pytest.raises(ValueError):
            remove_columns(dataset, columns_to_remove)
        
## Column Renaming Tests
rename_cases = [
    (
        'src/input_data/dataset_two.csv',
        {
            'id': 'client_identifier',
            'btc_a': 'bitcoin_address',
            'cc_t': 'credit_card_type'
        },
        ['client_identifier', 'bitcoin_address', 'credit_card_type', 'cc_n']
    ),
    (
        'src/input_data/dataset_one.csv',
        {
            'first_name': 'fname',
            'last_name': 'lname',
        },
        ['id', 'fname', 'lname', 'email', 'country']
    )
]

@pytest.mark.parametrize("file_path,mapping,expected_cols", rename_cases)
def test_column_renaming(spark_session, file_path, mapping, expected_cols):
    dataset = load_csv_in_spark(spark_session, file_path)
    
    renamed_df = rename_columns(dataset, mapping)
    assert sorted(renamed_df.columns) == sorted(expected_cols), "Column renaming failed"