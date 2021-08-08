from Spark import spark_query_executor as spark
import os
from MetricsTracker import cost_tracker

myspark = spark.Processor()
parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
artists_table_path = os.path.join(parent_path, 'data/csv/' + 'Artists.csv')
artworks_table_path = os.path.join(parent_path, 'data/csv/' + 'Artworks.csv')


myspark.add_database_table('Artists', artists_table_path)
myspark.add_database_table('Artworks', artworks_table_path)

parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
query_list = []
with open(parent_path + "/generated_queries/filtering_queries_100.txt", "r") as artist_queries:
    query_list = artist_queries.readlines()

for query in query_list:
    myspark.execute_query_with_metrics(query.strip('\n'))
cost_tracker.SparkTracker.log_experiment_results('Spark_filtering_100.txt')