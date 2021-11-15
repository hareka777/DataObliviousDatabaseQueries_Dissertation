## Implementation and evaluation of data-oblivious query processing algorithms

Welcome to my Masters Thesis!

This repository contains the Pyhton code I have created in order to complete this thesis. The main libraries I used in this project are the pandas, numpy and pyspark packages.

### Abstract

Modern companies need to routinely handle and process huge amount of data which
requires expensive hardware resources for their efficient processing. Therefore, cloud
services became extremely popular as they enable companies to use shared resources
to store and manipulate their data. Despite this huge advantage of the cloud, several
security concerns need to be addressed. To hide the content of the stored data, companies use encryption. However, even the sequence and frequency of data accesses
enables the cloud owner or attacker to infer sensitive information about the data. Dataoblivious algorithms are designed to hide these memory accesses at the cost of the
huge performance overhead. In this project, I am going to implement two state of
the art data-oblivious database query processing systems, Opaque and ObliDB and
evaluate and compare their performance. Additionally, I am going to extend these algorithms with a flexible block size feature that allows the algorithms to operate on data
blocks that contain multiple records in order to improve their performance. As a result
of the project, I found that flexible block size can significantly lower the cost of these
algorithms in several cases. Additionally, I was able to extend the experiment results
of the Opaque and ObliDB papers, this gave me the ability to analyse their strengths
and weaknesses further.

### Structure of the application
The following picture illustrates the structure and main components of the application.

![image](https://user-images.githubusercontent.com/37445999/141828099-6a4a2404-ed83-42c3-956b-19e95b9f498f.png)

The file structure of this application corresnponds to this chart's elements. 

The system's input is a set of SQL queries that are processed by three different, query executor units, the Opaque, ObliDB and Spark. These components use different query processing algorithms that we would like to analyse and compare. After the query executions finished, they return the result tables. The Query Parser component transforms the input SQL queries to provide input to the Opaque and ObliDB units and the Database component provides the database tables. 

To be able to compare the performance of the different query processing systems and their algorithms, the Performance Tracker component logs the data access patterns and the  execution times.

Finally, during the evaluation, the Evaluation unit creates chars, diagrams and table about the algorithms' performance based on the Performane Tracker unit's output and the Result Tables.
