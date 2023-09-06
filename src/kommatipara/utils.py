from typing import Dict, List, Union
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pathlib import Path

def create_spark_session(app_name: str) -> SparkSession:
    """
    Returns a spark session which can be used to perform
    spark sql operations
    
    :param app_name: Name of the spark application
    
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
    
    :param spark: The active spark session
    :param file_path: The path to the csv file to be loaded
    
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
    
    :param dataframe: The DataFrame to be filtered.
    :param column_name: The column on which to filter.
    :param filter: The list of items that should be present in the column.
    
    :returns: The filtered dataframe.
    """
    # Error handling
    if column_name not in dataframe.columns:
        raise ValueError("Column name not present in dataframe.")
    if not isinstance(filter_list, list):
        raise ValueError("Provided filter_list is not a list.")

    # Select the wanted rows using filter
    filtered_df = dataframe.filter(dataframe[column_name].isin(filter_list))
    return filtered_df

def remove_columns(dataframe: DataFrame, columns: Union[str, List[str]]) -> DataFrame:
    """
    Remove a selection of columns from a spark dataframe.
    
    :param dataframe: The dataframe from which the columns are removed.
    :param columns: The column(s) to be removed.
    
    :returns: The dataframe with the columns removed.
    """
    if isinstance(columns, str):
        if columns not in dataframe.columns:
            raise ValueError("Column to be dropped not present in dataframe.")
        # If the column is present, it can be dropped and the df returned.
        return dataframe.drop(columns)
    elif isinstance(columns, list):
        if not all([col in dataframe.columns for col in columns]):
            raise ValueError("Not all columns to be dropped are in the dataframe.")
        return dataframe.drop(*columns)
    else:
        raise ValueError("Columns to be dropped are not a string or list")
    
def rename_columns(dataframe: DataFrame, column_mapping: Dict[str, str]) -> DataFrame:
    """
    Rename columns of a dataframe using a mapping from old to new.
    
    :param dataframe: The dataframe of which columns will be renamed.
    :param column_mapping:
        A dictionary with keys of old column names and values as new column names.
    """
    
    # Now we generate a list of columns where the columns that are to be renamed
    # get an `.alias` added, and others are just selected as they are
    return dataframe.select(*(
            F.col(column).alias(column_mapping[column])
            if column in column_mapping.keys() 
            else F.col(column)
            for column in dataframe.columns
    ))