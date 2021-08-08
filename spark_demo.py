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
import pyspark
from pyspark.sql import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from Spark import spark_dataframe
from sparkmeasure import StageMetrics
import findspark
from database import table
from IPython.core.magic import (register_line_magic, register_cell_magic, register_line_cell_magic)

findspark.init()
#Load the DataFrame API session into Spark and create a session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
#spark.conf.set.config('spark.driver.extraClassPath', r'C:\Users\USER\miniconda3\pkgs\pyspark-3.1.1-pyhd3eb1b0_0\site-packages\pyspark\jars\spark.jars.packages.spark-measure_2.12-0.17')
#spark.conf.set.config('spark.executor.extraClassPath', r'C:\Users\USER\miniconda3\pkgs\pyspark-3.1.1-pyhd3eb1b0_0\site-packages\pyspark\jars\spark.jars.packages.spark-measure_2.12-0.17')

artists_table = (spark.read.format("csv").options(header="true")
    .load(os.path.join(os.getcwd(), 'data/csv/'+'Artists.csv')))

# registering data to the database
#mydf = spark_dataframe.SparkDataFrame(df)
artists_table.createOrReplaceTempView('Artists')
#artworks.createOrReplaceTempView('Artworks')

# executing query
#res = spark.sql('''SELECT * FROM table WHERE Truth=true ORDER BY Value ASC''')
#print(res)
# evaluating cost
# not res.explain().stats('simplestring')

stagemetrics = StageMetrics(spark)
stagemetrics.begin()
stagemetrics.runandmeasure(locals(), 'spark.sql("select DisplayName from Artists where ConstituentID=5").show()')
stagemetrics.end()

# evaluate cost
metrics = stagemetrics.create_stagemetrics_DF()
records_read = metrics.select('recordsRead').collect()
records_written = metrics.select('recordsWritten').collect()

records_read_sum = 0
for row in records_read:
    records_read_sum += row['recordsRead']

records_written_sum = 0
for row in records_written:
    records_written_sum += row['recordsWritten']

print('Total Records Read: ', records_read_sum)
print('Total Records Written: ', records_written_sum)
