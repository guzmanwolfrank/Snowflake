A collection of Snowflake Apps and Tools.


# Snowflake Data Analysis Project
![0_AmYXrtpALhMlQcZI](https://github.com/guzmanwolfrank/Snowflake-Python-SnowflakeSQL/assets/29739578/ba076e85-7870-45e3-bf57-3e826eef933d)

This project is aimed at performing data analysis on transactions stored in a Snowflake database. It involves uploading transaction data from a CSV file into a Snowflake table, executing SQL queries to analyze the data, and displaying the results.

## Features

- **Data Upload**: Upload transaction data from a CSV file into a Snowflake table.
- **Data Analysis**: Perform various data analysis tasks using SQL queries on the Snowflake database.
- **Result Visualization**: Display the results of the data analysis queries.

## Prerequisites

Before running the project, ensure you have the following installed:

- Python 3.x
- `snowflake-connector-python` library
- `pandas` library

## Project Files

- **README.md**: This file provides an overview of the project, including setup instructions, usage guidelines, and other relevant information.

- **SnowflakeQueries.ipynb**: This Jupyter Notebook contains a collection of SQL queries used for data analysis on transactions stored in a Snowflake database.

- **Transactions.csv**: This CSV file contains raw transaction data. It serves as the input for the Snowflake data analysis project.

- **generator.ipynb**: This Jupyter Notebook contains the Python code used to generate fake transaction data for testing and development purposes.

- **transactions.db**: This SQLite database file stores transaction data locally. It is used for testing and development purposes.

- **snowflake_script.py**: Main program. SQL queries used for data analysis on transactions stored in a Snowflake database.

    
## Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/your_username/snowflake-data-analysis.git
    cd snowflake-data-analysis
    ```

2. Install dependencies:

    ```bash
    pip install snowflake-connector-python pandas
    ```

3. Update Snowflake connection parameters:

    Open the `snowflake_script.py` file and replace the placeholders (`<your_user>`, `<your_password>`, `<your_account>`, etc.) with your actual Snowflake credentials.

## Usage

1. Place your CSV file containing transaction data in the project directory.

2. Run the `snowflake_script.py` script to upload the CSV file to a Snowflake table, execute the predefined SQL queries, and print the results:

    ```bash
    python snowflake_script.py
    ```

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any bugs, feature requests, or improvements.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

