import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import numpy as np
import lxml


def log_progress(message):
  ''' This function logs the mentioned message at a given stage of the code execution
      to a log file. 
      Function returns nothing'''
  timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
  now = datetime.now()  # get current timestamp
  timestamp = now.strftime(timestamp_format)
  with open("./code_log.txt", "a") as f:
    f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attributes):
  ''' This function aims to extract the required
  information from the website and save it to a data frame. The
  function returns the data frame for further processing. '''
  res = requests.get(url).content
  soup = BeautifulSoup(res, 'lxml')
  rows = soup.find('div', {
      'class': 'thumb tmulti tright'
  }).find_next().find_next().find_next().find_next().find_next().find_next(
  ).find_next().find_next().find_next().find_next().tbody.find_all('tr')

  banks, MC_USD_Billion = [], []
  for i in range(1, len(rows)):
    banks.append(rows[i].text.strip().split('\n')[2])
    MC_USD_Billion.append(rows[i].text.strip().split('\n')[-1])
  df = pd.DataFrame(list(zip(banks, MC_USD_Billion)), columns=table_attributes)
  df[table_attributes[-1]] = df[table_attributes[-1]].astype(float)
  return df


def transform(df, csv_path):
  ''' This function accesses the CSV file for exchange rate
  information, and adds three columns to the data frame, each
  containing the transformed version of Market Cap column to
  respective currencies'''
  dataframe = pd.read_csv(csv_path)
  exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
  df['MC_GBP_Billion'] = [
      np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']
  ]
  df['MC_EUR_Billion'] = [
      np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']
  ]
  df['MC_INR_Billion'] = [
      np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']
  ]
  return df


def load_to_csv(df, output_path):
  ''' This function saves the final data frame as a CSV file in
    the provided path.
    Function returns nothing.'''
  df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
  ''' This function saves the final data frame to a database
      table with the provided name. 
      Function returns nothing.'''
  df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
  '''
    This function runs the query on the database table and
    prints the output on the terminal.
    Function returns nothing.
    '''
  print('=>' + query_statement)
  query_output = pd.read_sql(query_statement, sql_connection)
  print(query_output)


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ['Name', 'MC_USD_Billion']
csv_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_progress('Preliminaries complete. Initiating ETL process')

################################################################################
try:
  # Extract the Data
  df = extract(url, table_attributes)
  log_progress('Data extraction complete. Initiating Transformation process')

  # Transform the Data
  df = transform(df, 'exchange_rate.csv')
  log_progress('Data transformation complete. Initiating Loading process')

  # Loading Dataframe to csv file
  load_to_csv(df, csv_path)
  log_progress('Data saved to CSV file')

  # Initialize the connection with the database
  conn = sqlite3.connect(db_name)
  log_progress('SQL Connection initiated')
  # Loading Dataframe into a database
  load_to_db(df, conn, table_name)
  log_progress('Data loaded to Database as a table, Executing queries')

  # Executing some queries on the database
  sql_statement1 = f'SELECT * FROM {table_name}'
  sql_statement2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
  sql_statement3 = f'SELECT Name from {table_name} LIMIT 5'
  run_query(sql_statement1, conn)
  run_query(sql_statement2, conn)
  run_query(sql_statement3, conn)
  log_progress('Process Complete')
except:
  print('Error Occurred, Please check the log file')
