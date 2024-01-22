# Code for ETL operations on World's Largest Banks data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

data_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
table_attribs_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"] # attributes of column names for the dataframe stored as a list
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'

def log_progress(message):
    # This function logs the mentioned message at a given stage of the code execution to a log file
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) # convert timestamp format to string
    with open("./code_log.txt","a") as file: 
        file.write(timestamp + ' : ' + message + '\n') # add each log entry in a new line in the txt file

def extract(data_url, table_attribs):
    # This function extracts the required information from the website and saves it to a dataframe
    # The function returns the dataframe for further processing
    page = requests.get(data_url).text # extract the webpage as text
    data = BeautifulSoup(page, 'html.parser') # parse the text into an HTML object
    df = pd.DataFrame(columns=table_attribs) # create a pandas DataFrame with column argument set as table_attribs
    data_dict = dict() # create an empty dictionary 
    Name_vals = [] # create an empty list that will contain the banks' names
    Market_cap_vals = [] # create an empty list that will contain the Market capitalization values
    tables = data.find_all('tbody') # find all the tables in the HTML code
    rows = tables[0].find_all('tr') # from table number 1 (at index 0), find all rows
    for row in rows:
        cells = row.find_all('td') # for each row, find all data cells
        for cell in cells:
            if len(cells) != 0: # check that entries are not empty
                Name = cells[1].find_all('a')[1].contents[0] # extract the text content of the index 1 cell,
                # after the second link ('a' tag) 
                Market_cap = float(cells[2].contents[0][:-1]) # extract the content of index 2 cell,
                # remove the last character, and typecast the value to float format
                if Name not in Name_vals: # we don't want any duplicates so we check if the bank name 
                # is already in the Name_vals list
                    Name_vals.append(Name)
                    Market_cap_vals.append(Market_cap)
                    data_dict = {"Name": Name, "MC_USD_Billion": Market_cap}
                    df1 = pd.DataFrame(data_dict, index=[0]) # create a dataframe from the dictionary 
                    df = pd.concat([df,df1], ignore_index=True) # combine dataframes
                else: 
                    continue
            else:
                continue
    return df

def extract_from_csv(file_to_process):
    csv_df = pd.read_csv(file_to_process, index_col=0)
    csv_dict = csv_df.to_dict()['Rate']
    return csv_dict

def transform(df):
    # This function transforms the Market Capitalization information according to exchange rate in GBP, EUR and INR,
    # rounded to 2 decimal places, and adds the respective columns to the dataframe
    # The function returns the transformed dataframe
    df['MC_GBP_Billion'] = [np.round(x*csv_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*csv_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*csv_dict['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, csv_path):
    # This function saves the final dataframe as a CSV file in the provided path
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    # This function saves the final dataframe as a database table
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    # This function runs the stated query on the database table and prints the output on the terminal 
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Extracting information
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(data_url, table_attribs)

# Transforming information
log_progress('Data extraction complete. Initiating Transformation process')

csv_dict = extract_from_csv('exchange_rate.csv')

df = transform(df)

# Loading information
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db') # Connecting to the SQLite3 database server

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

# Querying the database table 
log_progress('Data loaded to Database as a table, Executing queries')

# Query no. 1: Print the contents of the entire table
query_statement1 = f"SELECT * FROM Largest_banks"
run_query(query_statement1, sql_connection)

# Query no. 2: Print the average market capitalization of all the banks in Billion GBP
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement2, sql_connection)

# Query no. 3: Print only the names of the top 5 banks
query_statement3 = f"SELECT Name FROM Largest_banks LIMIT 5"
run_query(query_statement3, sql_connection)

log_progress('Process Complete.')

sql_connection.close() # Closing SQLite3 connection

log_progress('Server Connection closed')
