from pyspark.sql import SparkSession
import functions
import argparse

if __name__ == "__main__":
    ## Argument parsing ##
    parser = argparse.ArgumentParser(
        prog="python main.py",
        description="""
            A small ETL package for cleaning and combining
            client information with their financial details
        """,
        
    )
    # Set up the required arguments for the program
    parser.add_argument('client_csv')
    parser.add_argument('finance_csv')
    parser.add_argument('-f', '--filter', nargs='+', required=False)
    
    # Parse the arguments
    args = parser.parse_args()
    client_data_path = args.client_csv
    finance_data_path = args.finance_csv
    filter = args.filter
    
    ## Create Spark Session ## 
    spark = functions.create_spark_session("ABN_Challenge")
    
    ## Data Loading ## 
    client_data = functions.load_csv_in_spark(
        spark=spark,
        file_path=client_data_path
    )
    finance_data = functions.load_csv_in_spark(
        spark=spark,
        file_path=finance_data_path
    )
    