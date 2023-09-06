from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pathlib import Path

def create_spark_session(app_name: str) -> SparkSession:
    """
    Returns a spark session which can be used to perform
    spark sql operations
    
    :param str app_name: Name of the spark application
    
    :returns: A new active Spark Session
    """
    return (SparkSession.builder
            .master("local")
            .appName(app_name)
            .getOrCreate()
        )

def load_csv_in_spark(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Uses a spark session to read a csv file into a spark dataframe
    
    :param SparkSession spark: The active spark session
    :param str file_path: The path to the csv file to be loaded
    
    :returns: The file's contents in a spark DataFrame
    """
    assert Path(file_path).exists(), "File does not exist"
    return (spark.read
            .format("csv")
            .option("header", True)
            .load(file_path)
        )
    
def filter_column_by_list(
    dataframe: DataFrame, 
    column_name: str, 
    filter_list: list
) -> DataFrame:
    """
    Filter a Spark DataFrame based on the condition that the values
    from the given list are present in the given column.
    
    :param DataFrame dataframe: The DataFrame to be filtered.
    :param str column_name: The column on which to filter.
    :param list filter: The list of items that should be present in the column.
    
    :returns: The filtered dataframe.
    :rtype: DataFrame
    """
    # Error handling
    if column_name not in dataframe.columns:
        raise ValueError("Column name not present in dataframe.")
    if not isinstance(filter_list, list):
        raise ValueError("Provided filter_list is not a list.")

    # Select the wanted rows using filter
    filtered_df = dataframe.filter(dataframe[column_name].isin(filter_list))
    return filtered_df