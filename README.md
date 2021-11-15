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




