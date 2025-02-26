## Author : Yam Jason
import redis
import json
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.functions import col, date_format

class RedisHandler:
    """
    A class to handle storing and retrieving data from Redis.
    """
    def __init__(self, host='localhost', port=6379, db=0):
        """
        Initializes the Redis client.

        Parameters:
        - host: str, Redis server hostname.
        - port: int, Redis server port.
        - db: int, Redis database index.
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db)

    def store_dataframe(self, df, key_prefix="row"):
        """
        Stores a PySpark DataFrame in Redis as JSON strings.

        Parameters:
        - df: PySpark DataFrame, the DataFrame to store.
        - key_prefix: str, prefix for the Redis keys.
        """
        # Convert the Date column to a string format in the DataFrame
        df = df.withColumn("Date", date_format(col("Date"), "yyyy-MM-dd"))

        # Convert cleaned DataFrame to a list of dictionaries
        df_list = df.collect()
        data_to_store = [row.asDict() for row in df_list]

        # Store each row in Redis as a JSON string
        for i, data in enumerate(data_to_store):
            self.redis_client.set(f"{key_prefix}:{i}", json.dumps(data))

        print("Data stored in Redis successfully.")
        return len(data_to_store)

    def load_data(self, num_rows, key_prefix="row"):
        """
        Loads data from Redis and converts it to a list of dictionaries.

        Parameters:
        - num_rows: int, the number of rows to load.
        - key_prefix: str, prefix for the Redis keys.

        Returns:
        - list of dictionaries representing the loaded data.
        """
        data = []
        for i in range(num_rows):
            json_data = self.redis_client.get(f"{key_prefix}:{i}")
            if json_data:
                data.append(json.loads(json_data))
        
        # Convert the date strings back to date objects (if needed)
        for item in data:
            if 'Date' in item and item['Date']:
                item['Date'] = datetime.strptime(item['Date'], "%Y-%m-%d").date()
        
        return data

    def convert_to_dataframe(self, loaded_data, spark_session):
        """
        Converts the loaded data from Redis into a PySpark DataFrame.

        Parameters:
        - loaded_data: list of dictionaries, the data loaded from Redis.
        - spark_session: SparkSession object to create DataFrame.

        Returns:
        - PySpark DataFrame.
        """
        df = spark_session.createDataFrame([Row(**item) for item in loaded_data])
        return df
