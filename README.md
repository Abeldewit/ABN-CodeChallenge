# KommatiPara Client Data Collation

## Overview

This Python project is designed to collate and transform two separate datasets containing information about clients and their financial details for a fictitious company called KommatiPara, which deals with bitcoin trading. The goal of this project is to create a new dataset containing client emails from the United Kingdom and the Netherlands, along with relevant financial details, to support a marketing push.

**Note:** The data used in this project is entirely fake and is intended for demonstration purposes only.

## Requirements

- Python 3.8
- PySpark
- (Optional) Other dependencies specified in the requirements file

## Usage

To run this application, first install the kommatipara package.
```bash
python -m build ./src/
```

After installing the package, you can run the pipeline with the following command:

```bash
python -m kommatipara <client_data_file_path> <financial_data_file_path> <country_filter>
```

`<client_data_file_path>`: The path to the CSV file containing client information.
`<financial_data_file_path>`: The path to the CSV file containing financial information.
`<country_filter>`: The country filter to specify the target countries (e.g., "UK" or "Netherlands").

alternatively you can run the `__main__.py` file directly without installing the package. First install the project dependencies using:

```bash
python -m pip install -r requirements.txt
```

After this you can run the project using the following command:

```bash
python src/kommatipara/__main__.py <client_data_file_path> <financial_data_file_path> <country_filter>
```

## Project Structure
The project is organized as follows:

**main.py**: The main Python script to execute the data collation process.

**utils.p**y: Contains generic functions for data filtering and renaming.

**client_data/**: The directory where the output dataset will be saved.

**requirements.txt**: A file listing project dependencies.

**README.md**: This documentation file.

## Data Processing
1. The script reads the client and financial data from the specified files.
2. Personal identifiable information is removed from the client data, excluding emails.
3. Credit card numbers are removed from the financial data.
4. The data is filtered to include only clients from the specified countries (UK or Netherlands).
5. Column names are renamed for better readability as follows:
    - `id` is renamed to `client_identifier`.
    - btc_a is renamed to bitcoin_address.
    - cc_t is renamed to credit_card_type.
6. The filtered and renamed data is saved in the client_data directory.
7. 
## Logging
The application utilizes Loguru's logging module to provide information and error messages during execution.

## Bonus Features
- The project can be packaged into a source distribution file.
- A requirements file (requirements.txt) is provided to easily install project dependencies.
- Code is documented using docstrings in reStructuredText (reST) format to enhance code readability.