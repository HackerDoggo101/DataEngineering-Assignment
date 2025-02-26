## Author: Ashantha Rosary James 
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.functions import asc

class StarCountVisualizer:
    def __init__(self, spark_df: DataFrame):
        """
        Initialize the visualizer with a Spark DataFrame.

        Parameters:
        spark_df (DataFrame): Spark DataFrame with a 'StarCount' column.
        """
        self.spark_df = spark_df

    def prepare_data(self):
        """
        Group by StarCount, count occurrences, and collect data.
        """
        self.star_count_data = (
            self.spark_df
            .groupBy("StarCount")
            .count()
            .orderBy(asc("StarCount"))
            .collect()
        )

    def plot_pie_chart(self):
        """
        Create and display a pie chart based on the collected data.
        """
        if not hasattr(self, 'star_count_data'):
            raise ValueError("Data not found. Run 'prepare_data()' first.")
        
        # Extract StarCount and counts from the collected data
        star_counts = [row['StarCount'] for row in self.star_count_data]
        counts = [row['count'] for row in self.star_count_data]

        # Define labels for the pie chart
        labels = [f'Star Count {x}' for x in star_counts]

        # Create the pie chart
        plt.figure(figsize=(8, 8))
        plt.pie(counts, labels=labels, autopct='%1.1f%%', 
                colors=plt.cm.Paired(range(len(star_counts))), startangle=140)

        # Adding title
        plt.title('Distribution of Star Count')

        # Show plot
        plt.show()
