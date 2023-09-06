import country_converter as coco
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
    country_filter = args.filter
    
    if country_filter is not None:
        # Check if the arguments are country codes
        # (Codes are easier to chain in the CLI)
        if all([len(country) == 2 for country in country_filter]):
            # If so, we convert them to the full country name
            country_filter = coco.convert(
                names=country_filter,
                to='name_short'
            )
    
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
    
    ## Data Filtering ## 
    selected_client_data = functions.filter_column_by_list(
        dataframe=client_data,
        column_name='country',
        filter_list=country_filter
    )
    