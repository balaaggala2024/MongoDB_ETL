# tests/test_etl_pipeline.py

import unittest
import os
import sys
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np


from etl_pipeline import read_data, transform_data, insert_data_to_mongo

class TestETLPipeline(unittest.TestCase):
    
    def test_read_data(self):
      
        # Provide a sample file path for testing
        file_path = 'C:/Users/Documents/src_code/member_data.csv'

        # Sample data content for mock file reading
        data = read_data(file_path)
        
        # Check each item is a dictionary
        self.assertIsInstance(data[0], dict, "Each data item should be a dictionary")
        

    def test_transform_data(self):
      
        raw_data = [{
            'FirstName': ' John ',
            'LastName': 'Doe ',
            'Company': 'Example Corp',
            'BirthDate': '15011980',
            'Salary': '75000.00',
            'Address': '123 Main St',
            'Suburb': 'Anytown',
            'State': 'NSW',
            'Post': '2000',
            'Phone': '0123456789',
            'Mobile': '0987654321',
            'Email': 'john.doe@example.com'
        }]

        # Call the transform_data function
        transformed_df = transform_data(raw_data)

        # Check transformed DataFrame content
        self.assertEqual(len(transformed_df), 1, "Transformed data should contain one record")

        # Verify columns and data
        record = transformed_df.iloc[0]

        self.assertEqual(record['FullName'], 'John Doe', "FullName is not formatted correctly")
        self.assertEqual(record['BirthDate'], '15/01/1980', "BirthDate is not formatted correctly")
        self.assertEqual(record['Age'], 44, "Age is not calculated correctly")
        self.assertEqual(record['Salary'], 75000.00, "Salary is not converted to numeric correctly")
        self.assertEqual(record['SalaryBucket'], 'B', "SalaryBucket is not assigned correctly")

    import unittest
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from etl_pipeline import insert_data_to_mongo

class TestMongoDBInsertion(unittest.TestCase):
    
    def setUp(self):
        """Set up the MongoDB connection and test database/collection."""
        # Create a MongoDB client and a test database/collection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db_name = 'test_db'
        self.collection_name = 'test_collection'
        self.collection = self.client[self.db_name][self.collection_name]
        
        # Clear the collection before each test
        self.collection.delete_many({})

    def tearDown(self):
        """Clean up after each test by dropping the test database."""
        self.client.drop_database(self.db_name)
        self.client.close()

    def test_insert_data_to_mongo(self):
        """Test the insert_data_to_mongo function for successful insertion."""
        # Sample data to be inserted
        sample_data = pd.DataFrame([{
            'FullName': 'John Doe',
            'Company': 'Example Corp',
            'BirthDate': '15/01/1980',
            'Age': 44,
            'Salary': 75000.00,
            'SalaryBucket': 'B',
            'Address': '123 Main St',
            'Suburb': 'Anytown',
            'State': 'NSW',
            'Post': '2000',
            'Phone': '0123456789',
            'Mobile': '0987654321',
            'Email': 'john.doe@example.com'
        }])

        # Act: Insert data into MongoDB
        insert_data_to_mongo(sample_data, self.db_name, self.collection_name)

        # Assert: Check if the data has been inserted correctly
        inserted_records = list(self.collection.find({}))
        self.assertEqual(len(inserted_records), 1, "One record should be inserted")
        
        # Validate the content of the inserted record
        inserted_record = inserted_records[0]
        self.assertEqual(inserted_record['FullName'], 'John Doe')
        self.assertEqual(inserted_record['Company'], 'Example Corp')
        self.assertEqual(inserted_record['Age'], 44)

if __name__ == '__main__':
    unittest.main()
