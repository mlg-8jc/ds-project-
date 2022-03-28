# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 15:27:47 2022

@author: Mei Gilhousen 
"""
import pandas as pd
import numpy as np
import sqlalchemy
import os
import csv
import json
import pymongo
import pymysql
import mysql.connector
from sqlalchemy import create_engine


##Fetch / download / retrieve a remote data file by URL, or ingest a local file mounted.
'''
The following data is read in from a local file using pandas.
The data is obtained from kaggle and Yahoo Finance, 
and includes general financial information on 2310 ETFs, which can be compared by
differences in sizes, capitalization and returns.
This console will read in the data and give the user a summary of 
information from the dataset.
'''
'''
read in data from local path,
and assign connection to SQL
'''

data_dir = os.path.join(os.getcwd())
data_file = os.path.join(data_dir, 'ETFs.csv')
df = pd.read_csv(data_file, header=0, index_col=0)

host_name = "localhost"
ports = {"mysql" : 3306}

user_id = "root"
pwd = "meimei"

src_dbname = "datacleaning"
dst_dbname = "ETFs"

def get_sql_dataframe(user_id, pwd, host_name, db_name, sql_query):
    # Create a connection to the MySQL database
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)

    conn = sqlEngine.connect()
    dframe = pd.read_sql(sql_query, conn);
    conn.close()
    
    return dframe

def set_dataframe(user_id, pwd, host_name, db_name, df, table_name, pk_column, db_operation):
    # Create a connection to the MySQL database
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()

    if db_operation == "insert":
        df.to_sql(table_name, con=connection, index=False, if_exists='replace')
        sqlEngine.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});")
            
    elif db_operation == "update":
        df.to_sql(table_name, con=connection, index=False, if_exists='append')
    
    connection.close()
    
conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
sqlEngine = create_engine(conn_str, pool_recycle=3600)

sqlEngine.execute(f"DROP DATABASE IF EXISTS `{dst_dbname}`;")
sqlEngine.execute(f"CREATE DATABASE `{dst_dbname}`;")
sqlEngine.execute(f"USE {dst_dbname};")

'''
catch errors during SELECT statements 
'''

conn = pymysql.connect(host=host_name, user=user_id, password=pwd, database=src_dbname)
cursor = conn.cursor()

try:
    cursor.execute('SELECT * FROM fund_type;')
    
    for row in cursor.fetchmany(size=2):
        print(row)
        
    cursor.close()
    
except:
    print ("Error: unable to fetch data")
    
conn.close()

conn = pymysql.connect(host=host_name, user=user_id, password=pwd, database=src_dbname)
cursor = conn.cursor(pymysql.cursors.DictCursor)

try:
    cursor.execute('SELECT * FROM fund_symbol ORDER BY week52_low DESC LIMIT 5;')
    
    for row in cursor.fetchall():
        print(row)
        
    cursor.close()
    
except:
    print ("Error: unable to fetch all data")
    
conn.close()

conn = pymysql.connect(host=host_name, user=user_id, password=pwd, database=src_dbname)

df = pd.read_sql("SELECT * FROM ETFs ORDER BY week52_low DESC;", conn)

conn.close()
df.head()

'''
Show summary of the data, including summarization of numerical and 
categorical columns by count. 
The mean, std, min, quartiles, and maximum, of numerical columns
displayed, and total number of columns and rows.  
'''
numerical_cols = [col for col in df.columns if df.dtypes[col] != 'O']
categorical_cols = [col for col in df.columns if col not in numerical_cols]

print(numerical_cols)
print(categorical_cols)

df[numerical_cols].describe()

x=len(df.index)
y=len(df.columns)

print(" the number of records is "  + str (x) + " and the number of columns is " +  str (y))

'''
Convert the general format and data structure of the data source. 
CSV to JSON
'''

def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []
      
    #read csv file
    with open(csvFilePath, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            jsonArray.append(row)
  
    #convert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
        
'''
data output to CSV
'''
          
csvFilePath = r'ETFs.csv'
jsonFilePath = r'ETFs.json'
csv_to_json(csvFilePath, jsonFilePath)

data_dir = os.path.join(os.getcwd())
dest_file = os.path.join(data_dir, 'ETFs.csv')

df.to_csv(dest_file)


'''
Transforming the data. 
Reducing the number of columns from 142 to 8, in order to look at a filtered
version of the of the dataset, and specific variables of interest.
'''
cols = ['fund_category', 'fund_family','avg_vol_3month', 'week52_high_low_change',
'investment_strategy', 'fund_yield', 'size_type', 'fund_return_ytd']

new_data = df[cols]

x1=len(new_data.index)
y2=len(new_data.columns)

print(" the number of records is "  + str (x1) + " and the number of columns is " +  str (y2))   

'''
create key
'''
new_data.rename(columns={"fund_symbol":"etf_key"}, inplace=True)

'''
new table created
'''
initial_value = 1
new_data.insert(loc=0, column='etf_key', value=range(initial_value, len(new_data) +initial_value))

dataframe = new_data
table_name = 'new_data_table'
primary_key = 'etf_key'
db_operation = "insert"

set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)

'''
new table exported to SQL
'''


















