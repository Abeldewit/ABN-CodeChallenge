import utils
import country_converter as coco
from loguru import logger
import argparse

logger.add("./logs/kommatipara_pipe.log", rotation="10 minutes")


if __name__ == "__main__":
    logger.info("Started KommatiPara marketing pipeline")
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
    parser.add_argument(
        '-c', '--clean', 
        action='store_true',
        required=False
    )
    
    # Parse the arguments
    args = parser.parse_args()
    client_data_path = args.client_csv
    finance_data_path = args.finance_csv
    country_filter = args.filter
    clean_data = args.clean
    print(clean_data)
    
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
    spark = utils.create_spark_session("ABN_Challenge")
    
    ## Data Loading ## 
    client_data = utils.load_csv_in_spark(
        spark=spark,
        file_path=client_data_path
    )
    finance_data = utils.load_csv_in_spark(
        spark=spark,
        file_path=finance_data_path
    )
    
    ## Data Filtering ## 
    selected_client_data = utils.filter_column_by_list(
        dataframe=client_data,
        column_name='country',
        filter_list=country_filter
    )
    
    ## Sensitive Column Removal ##
    if not clean_data:
        clean_client_data = utils.remove_columns(
            dataframe=selected_client_data,
            columns=['first_name', 'last_name']
        )
        clean_finance_data = utils.remove_columns(
            dataframe=finance_data,
            columns='cc_n'
        )
    elif clean_data:
        # We mask the data instead of outright removing it
        clean_client_data = utils.mask_sensitive_data(
            dataframe=selected_client_data,
            columns=['last_name'],
            mask_start=1,  # Only retain the first letter of the name
            mask_char=' '
        )
        
        clean_finance_data = utils.mask_sensitive_data(
            dataframe=finance_data,
            columns='cc_n',
            mask_end=-4 # Only retrain the last four digits of the names
        )
    
    ## Join the two datasets ##
    joined_dataframe = clean_client_data.join(
        other=clean_finance_data,
        on='id',
        how='left' # only id's of selected countries
    )
    logger.trace("Joined datasets on 'id' column.")
    
    
    ## Rename Finance Columns ##
    column_mapping = {
        'id': 'client_identifier',
        'btc_a': 'bitcoin_address',
        'cc_t': 'credit_card_type',
        'cc_n': 'credit_card_number'
    }
    final_dataframe = utils.rename_columns(joined_dataframe, column_mapping)
    
    ## Output final dataframe ##
    output_file_path = './client_data/'
    final_dataframe\
        .write\
        .option('header', True)\
        .csv(
            path=output_file_path,
            mode='overwrite' # This option is used for testing but is normally dangerous
        )
    logger.success(f"Written final dataset to: {output_file_path}")
        
    
    