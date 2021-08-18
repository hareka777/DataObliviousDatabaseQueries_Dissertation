# import pyspark class Row from module sql
import os
import sys
# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = r'C:\opt\spark\spark-3.1.2-bin-hadoop3.2\spark-3.1.2-bin-hadoop3.2'
os.environ['HADOOP_HOME'] = r'C:\opt\spark\spark-3.1.2-bin-hadoop3.2\spark-3.1.2-bin-hadoop3.2\hadoop'
# os.environ['SPARK_HOME'] = "/home/jie/d2/spark-0.9.1"
# Append to PYTHONPATH so that pyspark could be found
sys.path.append(r"C:\opt\spark\spark-3.1.2-bin-hadoop3.2\spark-3.1.2-bin-hadoop3.2\python")
sys.path.append(r"C:\opt\spark\spark-3.1.2-bin-hadoop3.2\spark-3.1.2-bin-hadoop3.2\python\lib")
# sys.path.append("/home/jie/d2/spark-0.9.1/python")
# Now we are ready to import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print("Spark Modules are successfully imported")
except ImportError as e:
    print ("Spark Module importing errors", e)

import sys
sys.path.append(r"C:/opt/spark/spark-3.1.2-bin-hadoop3.2/spark-3.1.2-bin-hadoop3.2/python/lib")
from sparkmeasure import StageMetrics
import findspark
from pyspark.sql import SparkSession
from MetricsTracker import cost_tracker
import time

class Processor():
    spark = None
    artists_table_size = 15212
    artworks_table_size = 0
    def __init__(self):
        findspark.init()
        # Load the DataFrame API session into Spark and create a session
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
        # block size parameter is set based on the following post:
        # https://stackoverflow.com/questions/40954825/how-to-change-hdfs-block-size-in-pyspark
        #block_size = str(1024 * 1024 * 0.0001)
        #self.spark._jsc.hadoopConfiguration().set("dfs.block.size", block_size)


    def add_database_table(self, table_name, path=None):
        if path is None:
            created_table = (self.spark.read.format("csv").options(header="true")
                             .load(os.path.join(os.getcwd(), 'data/csv/' + table_name + '.csv')))
            created_table = created_table.limit(300)

            # registering data to the database
            created_table.createOrReplaceTempView(table_name)
        else:
            created_table = (self.spark.read.format("csv").options(header="true")
                             .load(path))
            created_table = created_table.limit(300)

            # registering data to the database
            created_table.createOrReplaceTempView(table_name)

    def execute_query_with_metrics(self, query):
        stagemetrics = StageMetrics(self.spark)
        stagemetrics.begin()
        start = time.time()
        stagemetrics.runandmeasure(locals(),
                                   'self.spark.sql("'+query+'").show()')
        end = time.time()
        stagemetrics.end()

        metrics = stagemetrics.create_stagemetrics_DF()
        aggregated_metrics = stagemetrics.aggregate_stagemetrics_DF()

        records_read = metrics.select('recordsRead').collect()
        records_written = metrics.select('recordsWritten').collect()
        execution_times = metrics.select('executorRunTime').collect()

        records_read_sum = 0
        for row in records_read:
            records_read_sum += row['recordsRead']

        records_written_sum = 0
        for row in records_written:
            records_written_sum += row['recordsWritten']

        execution_time_sum = 0
        for row in execution_times:
            execution_time_sum += row['executorRunTime']


        print('Total Records Read: ', records_read_sum)
        print('Total Records Written: ', records_written_sum)
        print('Total Execution Time: ', execution_time_sum)
        result = self.spark.sql(query)
        result = self.spark.sql(query)
        number_of_result = result.count()
        print('Number of results : ',number_of_result)

        total_cost = records_read_sum + records_written_sum + number_of_result
        cost_tracker.SparkTracker.register_cost(total_cost, records_read_sum, records_written_sum + number_of_result)
        cost_tracker.SparkTracker.register_execution_time(end - start)
