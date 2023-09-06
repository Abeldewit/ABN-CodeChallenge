from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


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
    return (spark.read
            .format("csv")
            .option("header", True)
            .load(file_path)
        )