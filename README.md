# Implemenatation and comparison of data oblivivous query processing algorithms
## MSc Project

This repository contains the code and experiment results that supports my MSc project.

### Project structure
The project has he followinng structure and contains the following units.

1. Data: The database tables that we used to execute the experiment.
2. Database: This component contains the objects represent the database, database tables etc.
3. Evaluation: This unit contains the code that executes the generated queries, the experiment. This folder also contains the experiment results, such as execution time, costs and memory access patterns.
4. Common building blocks: Low level methods that are commoly used in the whole project.
5. Generated queries: This unit contains the queries generated in order to execut the experiment.
6. MetricsTracker: This component collects and logs the performance metrics during the software running. These metrics are the cost of exeution, memory access patterns and the execution times.
7. Opaque: Implementation of Opaque's data-oblivious queries.
8. ObliDB: Implementation of ObliDB's data-oblivious queries.
9. Spark: Imports and uses the pyspark package to execute the database queries using Spark SQL.

### Installation guide
Most of the imported packages do not need any further installation. However importing Spark SQL need to be completed prior to running the code.

The following guide provides all the information needed to install and import pyspark and all its dependencies. After executing the steps listed and explained in the guide, everything is ready to run the code. 

https://bigdata-madesimple.com/guide-to-install-spark-and-use-pyspark-from-jupyter-in-windows/

#### Additional installation and general information and support
https://spark.apache.org/docs/latest/api/python/getting_started/install.html
https://medium.com/analytics-vidhya/installing-and-using-pyspark-on-windows-machine-59c2d64af76e
