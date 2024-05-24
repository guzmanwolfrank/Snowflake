import pandas as pd
import snowflake.connector
import os
import warnings

# Define a function to ignore specific warning types
def ignore_warnings():
    warnings.filterwarnings("ignore")

# Snowflake connection parameters
snowflake_conn_params = {
    'user': '<your_user>',
    'password': '<your_password>',
    'account': '<your_account>',
    'warehouse': '<your_warehouse>',
    'database': 'Transactions',
    'schema': 'public'
}

# Path to the CSV file
csv_file_path = 'transactions.csv'

# Establish connection to Snowflake
conn = snowflake.connector.connect(**snowflake_conn_params)
cursor = conn.cursor()

# Create database and table if they don't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS Transactions")
cursor.execute("USE DATABASE Transactions")
cursor.execute("CREATE SCHEMA IF NOT EXISTS public")
cursor.execute('''
CREATE TABLE IF NOT EXISTS Transactions (
    Date STRING,
    Time STRING,
    User STRING,
    UserID INT,
    Server_Location STRING,
    Input_Type STRING,
    Balance FLOAT,
    Amount_Transaction FLOAT,
    Fees FLOAT,
    API_Group STRING,
    Previous_Amount FLOAT,
    Final_Balance FLOAT,
    Account_Status STRING,
    Transaction_Status STRING,
    Error_Code INT
);
''')

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Insert data from DataFrame to Snowflake
# Convert DataFrame to a list of tuples
data = df.values.tolist()

# Create a Snowflake-compatible SQL INSERT statement
insert_query = '''
INSERT INTO Transactions (Date, Time, User, UserID, Server_Location, Input_Type, Balance, Amount_Transaction, Fees, API_Group, Previous_Amount, Final_Balance, Account_Status, Transaction_Status, Error_Code)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Insert each row from the DataFrame
for row in data:
    cursor.execute(insert_query, row)

# Commit the transaction
conn.commit()

# Define the 10 SQL queries
queries = [
    # Query 1: Select all transactions in 'Region 1'
    "SELECT * FROM Transactions WHERE Server_Location = 'Region 1';",
    
    # Query 2: Count transactions by each user type
    "SELECT User, COUNT(*) AS Transaction_Count FROM Transactions GROUP BY User;",
    
    # Query 3: Get the total transaction amount for each API group
    "SELECT API_Group, SUM(Amount_Transaction) AS Total_Amount FROM Transactions GROUP BY API_Group;",
    
    # Query 4: Find the average balance for active accounts
    "SELECT AVG(Balance) AS Average_Balance FROM Transactions WHERE Account_Status = 'Active';",
    
    # Query 5: List all denied transactions with error codes
    "SELECT * FROM Transactions WHERE Transaction_Status = 'Denied';",
    
    # Query 6: Calculate the total fees for transactions in 'Region 3'
    "SELECT SUM(Fees) AS Total_Fees FROM Transactions WHERE Server_Location = 'Region 3';",
    
    # Query 7: Get the maximum and minimum transaction amount
    "SELECT MAX(Amount_Transaction) AS Max_Transaction, MIN(Amount_Transaction) AS Min_Transaction FROM Transactions;",
    
    # Query 8: Count the number of transactions by transaction status
    "SELECT Transaction_Status, COUNT(*) AS Transaction_Count FROM Transactions GROUP BY Transaction_Status;",
    
    # Query 9: Find transactions with a balance greater than 1,000,000
    "SELECT * FROM Transactions WHERE Balance > 1000000;",
    
    # Query 10: List all transactions made via 'Mobile_Phone'
    "SELECT * FROM Transactions WHERE Input_Type = 'Mobile_Phone';"
]

# Execute each query and print the results
for i, query in enumerate(queries):
    cursor.execute(query)
    result = cursor.fetchall()
    print(f"Query {i+1} Results:")
    for row in result:
        print(row)
    print("\n")

# Close the cursor and connection
cursor.close()
conn.close()