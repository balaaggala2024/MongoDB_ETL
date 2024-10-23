# Import necessary libraries
import csv
import pandas as pd
from datetime import datetime 
from pymongo import MongoClient
import numpy as np

  # Define the Address class
class Address:
    def __init__(self, address):
        self.address = address


#task1 reads the file and return an in-memory data structure such as a list of dictionaries.
def read_data(source_file_path):
    # read the data from file
    data = []
    try:
        with open(source_file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file, delimiter='|')
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"Error: The file {source_file_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return data


#task 2 transformations using pandas  
def transform_data(data):
  try:
    df = pd.DataFrame(data)
    df.columns = ['FirstName', 'LastName', 'Company', 'BirthDate', 'Salary', 
                      'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email']

    default_date = pd.to_datetime('1900-01-01').strftime('%d/%m/%Y')  # Set your default date format

    df['BirthDate'] = pd.to_datetime(df['BirthDate'], format='%d%m%Y', errors='coerce').dt.strftime('%d/%m/%Y')
    df['BirthDate'].fillna(default_date, inplace=True) 
    df['FirstName'] = df['FirstName'].str.strip()    
    df['LastName'] = df['LastName'].str.strip()
   
    
    # Step 2: Merge FirstName and LastName into FullName
    df['FullName'] = df['FirstName'] + ' ' + df['LastName']

    
    # Step 3: Calculate Age as of March 1, 2024
    reference_date = datetime(2024, 3, 1)
    
    df['BirthDate'] = pd.to_datetime(df['BirthDate'], format='%d/%m/%Y')
   # df['Age'] = (reference_date - df['BirthDate']).dt.days // 365

    df['Age'] = df['BirthDate'].apply(lambda x: (reference_date - x).days // 365 if pd.notnull(x) else np.nan)
    # 5. Convert Salary to numeric and handle errors
    df['Salary'] = pd.to_numeric(df['Salary'], errors='coerce')

    # 6. Categorize SalaryBucket using pd.cut()
    bins = [-np.inf, 50000, 100000, np.inf]  # Define salary ranges
    labels = ['A', 'B', 'C']  # A for below 50k, B for 50-100k, C for above 100k
    df['SalaryBucket'] = pd.cut(df['Salary'], bins=bins, labels=labels, include_lowest=True)
    return df
 except ValueError as ve:
    print(f"ValueError: {ve}. Please check the input data format.")
 except Exception as e:
    print(f"An unexpected error occurred during transformation: {e}")



# Task 3 Insert Data into MongoDB 
def insert_data_to_mongo(dataframe, db_name, collection_name):
 try:
    # Establish MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')  # Adjust your MongoDB URI as necessary
    db = client[db_name]
    collection = db[collection_name]
    
    # Prepare data for insertion
    records = dataframe.to_dict(orient='records')
  
    
    # Insert records
    collection.insert_many(records)
    print(f"Inserted {len(records)} records into MongoDB.")
 except ConnectionFailure:
    print("Error: Could not connect to MongoDB. Please check your connection settings.")
 except Exception as e:
    print(f"An unexpected error occurred while inserting data into MongoDB: {e}")



# Main execution
if __name__ == "__main__":
    # Create employee DataFrame
    file_path = 'C:/Users/balaa/Documents/src_code/member_data.csv'  # Path to your data file
    employee_df = read_data(file_path)

    # Transform the data
    transformed_df = transform_data(employee_df) 

    # Insert transformed data into MongoDB
    insert_data_to_mongo(transformed_df, 'Mongo_ETL_DB', 'members')

    # Optional: Display transformed DataFrame
    print(transformed_df)