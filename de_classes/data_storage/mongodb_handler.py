# Author : Ashantha Rosary
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pprint

class MongoDBHandler:
    """
    A class to handle operations with MongoDB.
    """
    def __init__(self, uri, database, collection):
        """
        Initializes the MongoDB client with connection details.

        Parameters:
        - uri: str, URI for the MongoDB database.
        - database: str, name of the MongoDB database.
        - collection: str, name of the MongoDB collection.
        """
        self.uri = uri
        self.database = database
        self.collection = collection
        self.client = MongoClient(self.uri)
        self.db = self.client[self.database]
        self.collection = self.db[self.collection]

    def list_documents(self, limit=3):
        """
        Lists a specified number of documents from the MongoDB collection.

        Parameters:
        - limit: int, number of documents to list.
        """
        print(f"Listing {limit} documents in the {self.collection.name} collection: ")
        head_review = self.collection.find().limit(limit)
        pprint.pprint(list(head_review))

    def retrieve_data(self):
        """
        Retrieves all data from the MongoDB collection.

        Returns:
        - list of records retrieved from MongoDB.
        """
        mongo_data = list(self.collection.find())
        return mongo_data

    def convert_to_dataframe(self, mongo_data, spark_session, schema):
        """
        Converts MongoDB records into a PySpark DataFrame.

        Parameters:
        - mongo_data: list of records retrieved from MongoDB.
        - spark_session: SparkSession object to create DataFrame.
        - schema: StructType, schema definition for the DataFrame.

        Returns:
        - PySpark DataFrame.
        """
        # Remove the _id field from each record
        for record in mongo_data:
            if '_id' in record:
                del record['_id']

        df = spark_session.createDataFrame(mongo_data, schema=schema)
        return df

    def close(self):
        """
        Closes the MongoDB client connection.
        """
        if self.client is not None:
            self.client.close()
