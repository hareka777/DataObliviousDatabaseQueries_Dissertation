from Evaluation import Results
import os
import numpy as np
import seaborn as sns
import pandas as pd
import matplotlib. pyplot as plt

# parameter settings
block_sizes = [1, 3, 5, 10, 20]
oblivious_memory_sizes = [0.01, 0.05, 0.1, 0.2 ,0.3]

# separating all costs, read and write costs
def cost_processor(cost_lists):
    all_costs = []
    reading_costs = []
    writing_costs = []
    for cost in cost_lists:
        costs = cost.strip(']').strip('[').strip('\n').strip(']')
        cost = costs.split(',')
        all_costs.append(int(cost[0]))
        reading_costs.append(int(cost[1]))
        writing_costs.append(int(cost[2]))
    return all_costs, reading_costs, writing_costs

# reading in the Spark costs list
spark_filtering_all_costs, spark_filtering_reading_costs, spark_filtering_writing_costs = None, None, None
with open(os.getcwd() + "/Results/Spark/Filtering/Spark_filtering_100.txt_costs", "r") as spark_costs:
    spark_cost_lists = spark_costs.readlines()
    spark_filtering_all_costs, spark_filtering_reading_costs, spark_filtering_writing_costs = cost_processor(spark_cost_lists)

spark_filtering_all_costs = np.average(spark_filtering_all_costs)
spark_filtering_reading_costs = np.average(spark_filtering_all_costs)
spark_filtering_reading_costs = np.average(spark_filtering_writing_costs)

# reading in Opaque's cost lists
opaque_filtering_all_costs_block_1, opaque_filtering_reading_costs_block_1, opaque_filtering_writing_costs_block_1 = None, None, None
with open(os.getcwd() + "/Results/Opaque/Filtering/opaque_filtering_block_1.txt _costs", "r") as opaque_costs:
    opaque_cost_lists = opaque_costs.readlines()
    opaque_filtering_all_costs_block_1, opaque_filtering_reading_costs_block_1, opaque_filtering_writing_costs_block_1 = cost_processor(
        opaque_cost_lists)

opaque_filtering_all_costs_block_3, opaque_filtering_reading_costs_block_3, opaque_filtering_writing_costs_block_3 = None, None, None
with open(os.getcwd() + "/Results/Opaque/Filtering/opaque_filtering_block_3.txt _costs", "r") as opaque_costs:
    opaque_cost_lists = opaque_costs.readlines()
    opaque_filtering_all_costs_block_3, opaque_filtering_reading_costs_block_3, opaque_filtering_writing_costs_block_3 = cost_processor(
        opaque_cost_lists)

opaque_filtering_all_costs_block_5, opaque_filtering_reading_costs_block_5, opaque_filtering_writing_costs_block_5 = None, None, None
with open(os.getcwd() + "/Results/Opaque/Filtering/opaque_filtering_block_5.txt _costs", "r") as opaque_costs:
    opaque_cost_lists = opaque_costs.readlines()
    opaque_filtering_all_costs_block_5, opaque_filtering_reading_costs_block_5, opaque_filtering_writing_costs_block_5 = cost_processor(
        opaque_cost_lists)

opaque_filtering_all_costs_block_10, opaque_filtering_reading_costs_block_10, opaque_filtering_writing_costs_block_10 = None, None, None
with open(os.getcwd() + "/Results/Opaque/Filtering/opaque_filtering_block_10.txt _costs", "r") as opaque_costs:
    opaque_cost_lists = opaque_costs.readlines()
    opaque_filtering_all_costs_block_10, opaque_filtering_reading_costs_block_10, opaque_filtering_writing_costs_block_10 = cost_processor(
        opaque_cost_lists)

opaque_filtering_all_costs_block_20, opaque_filtering_reading_costs_block_20, opaque_filtering_writing_costs_block_20 = None, None, None
with open(os.getcwd() + "/Results/Opaque/Filtering/opaque_filtering_block_20.txt _costs", "r") as opaque_costs:
    opaque_cost_lists = opaque_costs.readlines()
    opaque_filtering_all_costs_block_20, opaque_filtering_reading_costs_block_20, opaque_filtering_writing_costs_block_20 = cost_processor(
        opaque_cost_lists)

# averaging the results
# Important to note: The costs are always the same as the execution is data-oblivious and deterministic.

opaque_filtering_all_costs_block_1 = np.average(opaque_filtering_all_costs_block_1)
opaque_filtering_reading_costs_block_1 = np.average(opaque_filtering_reading_costs_block_1)
opaque_filtering_writing_costs_block_1 = np.average(opaque_filtering_writing_costs_block_1)

opaque_filtering_all_costs_block_3 = np.average(opaque_filtering_all_costs_block_3)
opaque_filtering_reading_costs_block_3 = np.average(opaque_filtering_reading_costs_block_3)
opaque_filtering_writing_costs_block_3 = np.average(opaque_filtering_writing_costs_block_3)

opaque_filtering_all_costs_block_5 = np.average(opaque_filtering_all_costs_block_5)
opaque_filtering_reading_costs_block_5 = np.average(opaque_filtering_reading_costs_block_5)
opaque_filtering_writing_costs_block_5 = np.average(opaque_filtering_writing_costs_block_5)

opaque_filtering_all_costs_block_10 = np.average(opaque_filtering_all_costs_block_10)
opaque_filtering_reading_costs_block_10 = np.average(opaque_filtering_reading_costs_block_10)
opaque_filtering_writing_costs_block_10 = np.average(opaque_filtering_writing_costs_block_10)

opaque_filtering_all_costs_block_20 = np.average(opaque_filtering_all_costs_block_20)
opaque_filtering_reading_costs_block_20 = np.average(opaque_filtering_reading_costs_block_20)
opaque_filtering_writing_costs_block_20 = np.average(opaque_filtering_writing_costs_block_20)

# Opaque overall cost
all_costs_opaue_filtering = [opaque_filtering_all_costs_block_1, opaque_filtering_all_costs_block_3, opaque_filtering_all_costs_block_5, opaque_filtering_all_costs_block_10, opaque_filtering_all_costs_block_20]
reading_costs_opaque_filtering = [opaque_filtering_reading_costs_block_1, opaque_filtering_reading_costs_block_3, opaque_filtering_reading_costs_block_5, opaque_filtering_reading_costs_block_10, opaque_filtering_reading_costs_block_20]
writing_costs_opaque_filtering = [opaque_filtering_writing_costs_block_1, opaque_filtering_writing_costs_block_3, opaque_filtering_writing_costs_block_5, opaque_filtering_writing_costs_block_10, opaque_filtering_writing_costs_block_20]

# reading in ObliDB's Small filtering cost lists

# blocksize = 1 costs
oblidb_small_filtering_all_costs_block_1_mem_001, oblidb_small_filtering_reading_costs_block_1_obl_mem_001, oblidb_small_filtering_writing_costs_block_1_mem_001 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_1obl_mem_0.01.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_1_mem_001, oblidb_small_filtering_reading_costs_block_1_mem_001, oblidb_small_filtering_writing_costs_block_1_mem_001 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_1_mem_005, oblidb_small_filtering_reading_costs_block_1_mem_005, oblidb_small_filtering_writing_costs_block_1_mem_005 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_1obl_mem_0.05.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_1_mem_005, oblidb_small_filtering_reading_costs_block_1_mem_005, oblidb_small_filtering_writing_costs_block_1_mem_005 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_1_mem_01, oblidb_small_filtering_reading_costs_block_1_mem_01, oblidb_small_filtering_writing_costs_block_1_mem_01 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_1obl_mem_0.1.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_1_mem_01, oblidb_small_filtering_reading_costs_block_1_mem_01, oblidb_small_filtering_writing_costs_block_1_mem_01 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_1_mem_02, oblidb_small_filtering_reading_costs_block_1_mem_02, oblidb_small_filtering_writing_costs_block_1_mem_02 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_1obl_mem_0.2.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_1_mem_02, oblidb_small_filtering_reading_costs_block_1_mem_02, oblidb_small_filtering_writing_costs_block_1_mem_02 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_1_mem_03, oblidb_small_filtering_reading_costs_block_1_mem_03, oblidb_small_filtering_writing_costs_block_1_mem_03 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_1obl_mem_0.3.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_1_mem_03, oblidb_small_filtering_reading_costs_block_1_mem_03, oblidb_small_filtering_writing_costs_block_1_mem_03 = cost_processor(
        oblidb_small_cost_lists)

# blocksize = 3 costs
oblidb_small_filtering_all_costs_block_3_mem_001, oblidb_small_filtering_reading_costs_block_3_mem_001, oblidb_small_filtering_writing_costs_block_3_mem_001 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_3obl_mem_0.01.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_3_mem_001, oblidb_small_filtering_reading_costs_block_3_mem_001, oblidb_small_filtering_writing_costs_block_3_mem_001 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_3_mem_005, oblidb_small_filtering_reading_costs_block_3_mem_005, oblidb_small_filtering_writing_costs_block_3_mem_005 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_3obl_mem_0.05.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_3_mem_005, oblidb_small_filtering_reading_costs_block_3_mem_005, oblidb_small_filtering_writing_costs_block_3_mem_005 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_3_mem_01, oblidb_small_filtering_reading_costs_block_3_mem_01, oblidb_small_filtering_writing_costs_block_3_mem_01 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_3obl_mem_0.1.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_3_mem_01, oblidb_small_filtering_reading_costs_block_3_mem_01, oblidb_small_filtering_writing_costs_block_3_mem_01 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_3_mem_02, oblidb_small_filtering_reading_costs_block_3_mem_02, oblidb_small_filtering_writing_costs_block_3_mem_02 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_3obl_mem_0.2.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_3_mem_02, oblidb_small_filtering_reading_costs_block_3_mem_02, oblidb_small_filtering_writing_costs_block_3_mem_02 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_3_mem_03, oblidb_small_filtering_reading_costs_block_3_mem_03, oblidb_small_filtering_writing_costs_block_3_mem_03 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_3obl_mem_0.3.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_3_mem_03, oblidb_small_filtering_reading_costs_block_3_mem_03, oblidb_small_filtering_writing_costs_block_3_mem_03 = cost_processor(
        oblidb_small_cost_lists)

# blocksize = 5 costs
oblidb_small_filtering_all_costs_block_5_mem_001, oblidb_small_filtering_reading_costs_block_5_mem_001, oblidb_small_filtering_writing_costs_block_5_mem_001 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_5obl_mem_0.01.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_5_mem_001, oblidb_small_filtering_reading_costs_block_5_mem_001, oblidb_small_filtering_writing_costs_block_5_mem_001 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_5_mem_005, oblidb_small_filtering_reading_costs_block_5_mem_005, oblidb_small_filtering_writing_costs_block_5_mem_005 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_5obl_mem_0.05.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_5_mem_005, oblidb_small_filtering_reading_costs_block_5_mem_005, oblidb_small_filtering_writing_costs_block_5_mem_005 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_5_mem_01, oblidb_small_filtering_reading_costs_block_5_mem_01, oblidb_small_filtering_writing_costs_block_5_mem_01 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_5obl_mem_0.1.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_5_mem_01, oblidb_small_filtering_reading_costs_block_5_mem_01, oblidb_small_filtering_writing_costs_block_5_mem_01 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_5_mem_02, oblidb_small_filtering_reading_costs_block_5_mem_02, oblidb_small_filtering_writing_costs_block_5_mem_02 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_5obl_mem_0.2.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_5_mem_02, oblidb_small_filtering_reading_costs_block_5_mem_02, oblidb_small_filtering_writing_costs_block_5_mem_02 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_5_mem_03, oblidb_small_filtering_reading_costs_block_5_mem_03, oblidb_small_filtering_writing_costs_block_5_mem_03 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_5obl_mem_0.3.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_5_mem_03, oblidb_small_filtering_reading_costs_block_5_mem_03, oblidb_small_filtering_writing_costs_block_5_mem_03 = cost_processor(
        oblidb_small_cost_lists)

# blocksize = 10 costs
oblidb_small_filtering_all_costs_block_10_mem_001, oblidb_small_filtering_reading_costs_block_10_mem_001, oblidb_small_filtering_writing_costs_block_10_mem_001 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_10obl_mem_0.01.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_10_mem_001, oblidb_small_filtering_reading_costs_block_10_mem_001, oblidb_small_filtering_writing_costs_block_10_mem_001 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_10_mem_005, oblidb_small_filtering_reading_costs_block_10_mem_005, oblidb_small_filtering_writing_costs_block_10_mem_005 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_10obl_mem_0.05.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_10_mem_005, oblidb_small_filtering_reading_costs_block_10_mem_005, oblidb_small_filtering_writing_costs_block_10_mem_005 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_10_mem_01, oblidb_small_filtering_reading_costs_block_10_mem_01, oblidb_small_filtering_writing_costs_block_10_mem_01 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_10obl_mem_0.1.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_10_mem_01, oblidb_small_filtering_reading_costs_block_10_mem_01, oblidb_small_filtering_writing_costs_block_10_mem_01 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_10_mem_02, oblidb_small_filtering_reading_costs_block_10_mem_02, oblidb_small_filtering_writing_costs_block_10_mem_02 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_10obl_mem_0.2.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_10_mem_02, oblidb_small_filtering_reading_costs_block_10_mem_02, oblidb_small_filtering_writing_costs_block_10_mem_02 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_10_mem_03, oblidb_small_filtering_reading_costs_block_10_mem_03, oblidb_small_filtering_writing_costs_block_10_mem_03 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_10obl_mem_0.3.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_10_mem_03, oblidb_small_filtering_reading_costs_block_10_mem_03, oblidb_small_filtering_writing_costs_block_10_mem_03 = cost_processor(
        oblidb_small_cost_lists)

# blocksize = 20 costs
oblidb_small_filtering_all_costs_block_20_mem_001, oblidb_small_filtering_reading_costs_block_20_mem_001, oblidb_small_filtering_writing_costs_block_20_mem_001 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_20obl_mem_0.01.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_20_mem_001, oblidb_small_filtering_reading_costs_block_20_mem_001, oblidb_small_filtering_writing_costs_block_20_mem_001 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_20_mem_005, oblidb_small_filtering_reading_costs_block_20_mem_005, oblidb_small_filtering_writing_costs_block_20_mem_005 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_20obl_mem_0.05.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_20_mem_005, oblidb_small_filtering_reading_costs_block_20_mem_005, oblidb_small_filtering_writing_costs_block_20_mem_005 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_20_mem_01, oblidb_small_filtering_reading_costs_block_20_mem_01, oblidb_small_filtering_writing_costs_block_20_mem_01 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_20obl_mem_0.1.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_20_mem_01, oblidb_small_filtering_reading_costs_block_20_mem_01, oblidb_small_filtering_writing_costs_block_20_mem_01 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_20_mem_02, oblidb_small_filtering_reading_costs_block_20_mem_02, oblidb_small_filtering_writing_costs_block_20_mem_02 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_20obl_mem_0.2.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_20_mem_02, oblidb_small_filtering_reading_costs_block_20_mem_02, oblidb_small_filtering_writing_costs_block_20_mem_02 = cost_processor(
        oblidb_small_cost_lists)

oblidb_small_filtering_all_costs_block_20_mem_03, oblidb_small_filtering_reading_costs_block_20_mem_03, oblidb_small_filtering_writing_costs_block_20_mem_03 = None, None, None
with open(os.getcwd() + "/Results/ObliDB/Filtering/Small/oblidb_filtering_small_block_20obl_mem_0.3.txt _costs", "r") as oblidb_small_costs:
    oblidb_small_cost_lists = oblidb_small_costs.readlines()
    oblidb_small_filtering_all_costs_block_20_mem_03, oblidb_small_filtering_reading_costs_block_20_mem_03, oblidb_small_filtering_writing_costs_block_20_mem_03 = cost_processor(
        oblidb_small_cost_lists)

# averaging the costs for ObliDB small
# memory = 0.01MB
oblidb_small_filtering_all_costs_block_1_mem_001 = np.average(oblidb_small_filtering_all_costs_block_1_mem_001)
oblidb_small_filtering_reading_costs_block_1_mem_001 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_001)
oblidb_small_filtering_writing_costs_block_1_mem_001 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_001)

oblidb_small_filtering_all_costs_block_3_mem_001 = np.average(oblidb_small_filtering_all_costs_block_3_mem_001)
oblidb_small_filtering_reading_costs_block_3_mem_001 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_001)
oblidb_small_filtering_writing_costs_block_3_mem_001 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_001)

oblidb_small_filtering_all_costs_block_5_mem_001 = np.average(oblidb_small_filtering_all_costs_block_5_mem_001)
oblidb_small_filtering_reading_costs_block_5_mem_001 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_001)
oblidb_small_filtering_writing_costs_block_5_mem_001 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_001)

oblidb_small_filtering_all_costs_block_10_mem_001 = np.average(oblidb_small_filtering_all_costs_block_10_mem_001)
oblidb_small_filtering_reading_costs_block_10_mem_001 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_001)
oblidb_small_filtering_writing_costs_block_10_mem_001 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_001)

oblidb_small_filtering_all_costs_block_20_mem_001 = np.average(oblidb_small_filtering_all_costs_block_20_mem_001)
oblidb_small_filtering_reading_costs_block_20_mem_001 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_001)
oblidb_small_filtering_writing_costs_block_20_mem_001 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_001)

# memory = 0.05MB
oblidb_small_filtering_all_costs_block_1_mem_005 = np.average(oblidb_small_filtering_all_costs_block_1_mem_005)
oblidb_small_filtering_reading_costs_block_1_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_005)
oblidb_small_filtering_writing_costs_block_1_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_005)

oblidb_small_filtering_all_costs_block_3_mem_005 = np.average(oblidb_small_filtering_all_costs_block_3_mem_005)
oblidb_small_filtering_reading_costs_block_3_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_005)
oblidb_small_filtering_writing_costs_block_3_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_005)

oblidb_small_filtering_all_costs_block_5_mem_005 = np.average(oblidb_small_filtering_all_costs_block_5_mem_005)
oblidb_small_filtering_reading_costs_block_5_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_005)
oblidb_small_filtering_writing_costs_block_5_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_005)

oblidb_small_filtering_all_costs_block_10_mem_005 = np.average(oblidb_small_filtering_all_costs_block_10_mem_005)
oblidb_small_filtering_reading_costs_block_10_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_005)
oblidb_small_filtering_writing_costs_block_10_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_005)

oblidb_small_filtering_all_costs_block_20_mem_005 = np.average(oblidb_small_filtering_all_costs_block_20_mem_005)
oblidb_small_filtering_reading_costs_block_20_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_005)
oblidb_small_filtering_writing_costs_block_20_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_005)

# memory = 0.1MB
oblidb_small_filtering_all_costs_block_1_mem_01 = np.average(oblidb_small_filtering_all_costs_block_1_mem_01)
oblidb_small_filtering_reading_costs_block_1_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_01)
oblidb_small_filtering_writing_costs_block_1_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_01)

oblidb_small_filtering_all_costs_block_3_mem_01 = np.average(oblidb_small_filtering_all_costs_block_3_mem_01)
oblidb_small_filtering_reading_costs_block_3_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_01)
oblidb_small_filtering_writing_costs_block_3_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_01)

oblidb_small_filtering_all_costs_block_5_mem_01 = np.average(oblidb_small_filtering_all_costs_block_5_mem_01)
oblidb_small_filtering_reading_costs_block_5_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_01)
oblidb_small_filtering_writing_costs_block_5_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_01)

oblidb_small_filtering_all_costs_block_10_mem_01 = np.average(oblidb_small_filtering_all_costs_block_10_mem_01)
oblidb_small_filtering_reading_costs_block_10_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_01)
oblidb_small_filtering_writing_costs_block_10_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_01)

oblidb_small_filtering_all_costs_block_20_mem_01 = np.average(oblidb_small_filtering_all_costs_block_20_mem_01)
oblidb_small_filtering_reading_costs_block_20_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_01)
oblidb_small_filtering_writing_costs_block_20_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_01)

# memory = 0.2MB
oblidb_small_filtering_all_costs_block_1_mem_02 = np.average(oblidb_small_filtering_all_costs_block_1_mem_02)
oblidb_small_filtering_reading_costs_block_1_mem_02 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_02)
oblidb_small_filtering_writing_costs_block_1_mem_02 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_02)

oblidb_small_filtering_all_costs_block_3_mem_02 = np.average(oblidb_small_filtering_all_costs_block_3_mem_02)
oblidb_small_filtering_reading_costs_block_3_mem_02 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_02)
oblidb_small_filtering_writing_costs_block_3_mem_02 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_02)

oblidb_small_filtering_all_costs_block_5_mem_02 = np.average(oblidb_small_filtering_all_costs_block_5_mem_02)
oblidb_small_filtering_reading_costs_block_5_mem_02 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_02)
oblidb_small_filtering_writing_costs_block_5_mem_02 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_02)

oblidb_small_filtering_all_costs_block_10_mem_02 = np.average(oblidb_small_filtering_all_costs_block_10_mem_02)
oblidb_small_filtering_reading_costs_block_10_mem_02 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_02)
oblidb_small_filtering_writing_costs_block_10_mem_02 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_02)

oblidb_small_filtering_all_costs_block_20_mem_02 = np.average(oblidb_small_filtering_all_costs_block_20_mem_02)
oblidb_small_filtering_reading_costs_block_20_mem_02 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_02)
oblidb_small_filtering_writing_costs_block_20_mem_02 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_02)

# memory = 0.05MB
oblidb_small_filtering_all_costs_block_1_mem_005 = np.average(oblidb_small_filtering_all_costs_block_1_mem_005)
oblidb_small_filtering_reading_costs_block_1_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_005)
oblidb_small_filtering_writing_costs_block_1_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_005)

oblidb_small_filtering_all_costs_block_3_mem_005 = np.average(oblidb_small_filtering_all_costs_block_3_mem_005)
oblidb_small_filtering_reading_costs_block_3_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_005)
oblidb_small_filtering_writing_costs_block_3_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_005)

oblidb_small_filtering_all_costs_block_5_mem_005 = np.average(oblidb_small_filtering_all_costs_block_5_mem_005)
oblidb_small_filtering_reading_costs_block_5_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_005)
oblidb_small_filtering_writing_costs_block_5_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_005)

oblidb_small_filtering_all_costs_block_10_mem_005 = np.average(oblidb_small_filtering_all_costs_block_10_mem_005)
oblidb_small_filtering_reading_costs_block_10_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_005)
oblidb_small_filtering_writing_costs_block_10_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_005)

oblidb_small_filtering_all_costs_block_20_mem_005 = np.average(oblidb_small_filtering_all_costs_block_20_mem_005)
oblidb_small_filtering_reading_costs_block_20_mem_005 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_005)
oblidb_small_filtering_writing_costs_block_20_mem_005 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_005)

# memory = 0.1MB
oblidb_small_filtering_all_costs_block_1_mem_01 = np.average(oblidb_small_filtering_all_costs_block_1_mem_01)
oblidb_small_filtering_reading_costs_block_1_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_01)
oblidb_small_filtering_writing_costs_block_1_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_01)

oblidb_small_filtering_all_costs_block_3_mem_01 = np.average(oblidb_small_filtering_all_costs_block_3_mem_01)
oblidb_small_filtering_reading_costs_block_3_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_01)
oblidb_small_filtering_writing_costs_block_3_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_01)

oblidb_small_filtering_all_costs_block_5_mem_01 = np.average(oblidb_small_filtering_all_costs_block_5_mem_01)
oblidb_small_filtering_reading_costs_block_5_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_01)
oblidb_small_filtering_writing_costs_block_5_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_01)

oblidb_small_filtering_all_costs_block_10_mem_01 = np.average(oblidb_small_filtering_all_costs_block_10_mem_01)
oblidb_small_filtering_reading_costs_block_10_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_01)
oblidb_small_filtering_writing_costs_block_10_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_01)

oblidb_small_filtering_all_costs_block_20_mem_01 = np.average(oblidb_small_filtering_all_costs_block_20_mem_01)
oblidb_small_filtering_reading_costs_block_20_mem_01 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_01)
oblidb_small_filtering_writing_costs_block_20_mem_01 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_01)

# memory = 0.2MB
oblidb_small_filtering_all_costs_block_1_mem_03 = np.average(oblidb_small_filtering_all_costs_block_1_mem_03)
oblidb_small_filtering_reading_costs_block_1_mem_03 = np.average(oblidb_small_filtering_reading_costs_block_1_mem_03)
oblidb_small_filtering_writing_costs_block_1_mem_03 = np.average(oblidb_small_filtering_writing_costs_block_1_mem_03)

oblidb_small_filtering_all_costs_block_3_mem_03 = np.average(oblidb_small_filtering_all_costs_block_3_mem_03)
oblidb_small_filtering_reading_costs_block_3_mem_03 = np.average(oblidb_small_filtering_reading_costs_block_3_mem_03)
oblidb_small_filtering_writing_costs_block_3_mem_03 = np.average(oblidb_small_filtering_writing_costs_block_3_mem_03)

oblidb_small_filtering_all_costs_block_5_mem_03 = np.average(oblidb_small_filtering_all_costs_block_5_mem_03)
oblidb_small_filtering_reading_costs_block_5_mem_03 = np.average(oblidb_small_filtering_reading_costs_block_5_mem_03)
oblidb_small_filtering_writing_costs_block_5_mem_03 = np.average(oblidb_small_filtering_writing_costs_block_5_mem_03)

oblidb_small_filtering_all_costs_block_10_mem_03 = np.average(oblidb_small_filtering_all_costs_block_10_mem_03)
oblidb_small_filtering_reading_costs_block_10_mem_03 = np.average(oblidb_small_filtering_reading_costs_block_10_mem_03)
oblidb_small_filtering_writing_costs_block_10_mem_03 = np.average(oblidb_small_filtering_writing_costs_block_10_mem_03)

oblidb_small_filtering_all_costs_block_20_mem_03 = np.average(oblidb_small_filtering_all_costs_block_20_mem_03)
oblidb_small_filtering_reading_costs_block_20_mem_03 = np.average(oblidb_small_filtering_reading_costs_block_20_mem_03)
oblidb_small_filtering_writing_costs_block_20_mem_03 = np.average(oblidb_small_filtering_writing_costs_block_20_mem_03)
# ObliDB Small overall costs
oblidb_small_all_costs = {}
oblidb_small_reading_costs = {}
oblidb_small_writing_costs = {}

oblidb_small_all_costs[str(0.01)] = [oblidb_small_filtering_all_costs_block_1_mem_001, oblidb_small_filtering_all_costs_block_3_mem_001,
                                oblidb_small_filtering_all_costs_block_5_mem_001, oblidb_small_filtering_all_costs_block_10_mem_001,
                                oblidb_small_filtering_all_costs_block_20_mem_001]
oblidb_small_reading_costs[str(0.01)] = [oblidb_small_filtering_reading_costs_block_1_mem_001, oblidb_small_filtering_reading_costs_block_3_mem_001,
                                oblidb_small_filtering_reading_costs_block_5_mem_001, oblidb_small_filtering_reading_costs_block_10_mem_001,
                                oblidb_small_filtering_reading_costs_block_20_mem_001]
oblidb_small_writing_costs[str(0.01)] = [oblidb_small_filtering_writing_costs_block_1_mem_001, oblidb_small_filtering_writing_costs_block_3_mem_001,
                                oblidb_small_filtering_writing_costs_block_5_mem_001, oblidb_small_filtering_writing_costs_block_10_mem_001,
                                oblidb_small_filtering_writing_costs_block_20_mem_001]

oblidb_small_all_costs[str(0.05)] = [oblidb_small_filtering_all_costs_block_1_mem_005, oblidb_small_filtering_all_costs_block_3_mem_005,
                                oblidb_small_filtering_all_costs_block_5_mem_005, oblidb_small_filtering_all_costs_block_10_mem_005,
                                oblidb_small_filtering_all_costs_block_20_mem_005]
oblidb_small_reading_costs[str(0.05)] = [oblidb_small_filtering_reading_costs_block_1_mem_005, oblidb_small_filtering_reading_costs_block_3_mem_005,
                                oblidb_small_filtering_reading_costs_block_5_mem_005, oblidb_small_filtering_reading_costs_block_10_mem_005,
                                oblidb_small_filtering_reading_costs_block_20_mem_005]
oblidb_small_writing_costs[str(0.05)] = [oblidb_small_filtering_writing_costs_block_1_mem_005, oblidb_small_filtering_writing_costs_block_3_mem_005,
                                oblidb_small_filtering_writing_costs_block_5_mem_005, oblidb_small_filtering_writing_costs_block_10_mem_005,
                                oblidb_small_filtering_writing_costs_block_20_mem_005]

oblidb_small_all_costs[str(0.1)] = [oblidb_small_filtering_all_costs_block_1_mem_01, oblidb_small_filtering_all_costs_block_3_mem_01,
                                oblidb_small_filtering_all_costs_block_5_mem_01, oblidb_small_filtering_all_costs_block_10_mem_01,
                                oblidb_small_filtering_all_costs_block_20_mem_01]
oblidb_small_reading_costs[str(0.1)] = [oblidb_small_filtering_reading_costs_block_1_mem_01, oblidb_small_filtering_reading_costs_block_3_mem_01,
                                oblidb_small_filtering_reading_costs_block_5_mem_01, oblidb_small_filtering_reading_costs_block_10_mem_01,
                                oblidb_small_filtering_reading_costs_block_20_mem_01]
oblidb_small_writing_costs[str(0.1)] = [oblidb_small_filtering_writing_costs_block_1_mem_01, oblidb_small_filtering_writing_costs_block_3_mem_01,
                                oblidb_small_filtering_writing_costs_block_5_mem_01, oblidb_small_filtering_writing_costs_block_10_mem_01,
                                oblidb_small_filtering_writing_costs_block_20_mem_01]

oblidb_small_all_costs[str(0.2)] = [oblidb_small_filtering_all_costs_block_1_mem_02, oblidb_small_filtering_all_costs_block_3_mem_02,
                                oblidb_small_filtering_all_costs_block_5_mem_02, oblidb_small_filtering_all_costs_block_10_mem_02,
                                oblidb_small_filtering_all_costs_block_20_mem_02]
oblidb_small_reading_costs[str(0.2)] = [oblidb_small_filtering_reading_costs_block_1_mem_02, oblidb_small_filtering_reading_costs_block_3_mem_02,
                                oblidb_small_filtering_reading_costs_block_5_mem_02, oblidb_small_filtering_reading_costs_block_10_mem_02,
                                oblidb_small_filtering_reading_costs_block_20_mem_02]
oblidb_small_writing_costs[str(0.2)] = [oblidb_small_filtering_writing_costs_block_1_mem_02, oblidb_small_filtering_writing_costs_block_3_mem_02,
                                oblidb_small_filtering_writing_costs_block_5_mem_02, oblidb_small_filtering_writing_costs_block_10_mem_02,
                                oblidb_small_filtering_writing_costs_block_20_mem_02]

oblidb_small_all_costs[str(0.3)] = [oblidb_small_filtering_all_costs_block_1_mem_03, oblidb_small_filtering_all_costs_block_3_mem_03,
                                oblidb_small_filtering_all_costs_block_5_mem_03, oblidb_small_filtering_all_costs_block_10_mem_03,
                                oblidb_small_filtering_all_costs_block_20_mem_03]
oblidb_small_reading_costs[str(0.3)] = [oblidb_small_filtering_reading_costs_block_1_mem_02, oblidb_small_filtering_reading_costs_block_3_mem_02,
                                oblidb_small_filtering_reading_costs_block_5_mem_02, oblidb_small_filtering_reading_costs_block_10_mem_02,
                                oblidb_small_filtering_reading_costs_block_20_mem_02]
oblidb_small_writing_costs[str(0.3)] = [oblidb_small_filtering_writing_costs_block_1_mem_03, oblidb_small_filtering_writing_costs_block_3_mem_03,
                                oblidb_small_filtering_writing_costs_block_5_mem_03, oblidb_small_filtering_writing_costs_block_10_mem_03,
                                oblidb_small_filtering_writing_costs_block_20_mem_03]

# ObliDB Large overall costs
oblidb_large_all_costs = {}
oblidb_large_reading_costs = {}
oblidb_large_writing_costs = {}

oblidb_large_all_costs[str(0.01)] = [20000, 6668, 4000, 2000, 1000]
oblidb_large_reading_costs[str(0.01)] = [10000, 3334, 2000, 1000, 500]
oblidb_large_writing_costs[str(0.01)] = [10000, 3334, 2000, 1000, 500]

oblidb_large_all_costs[str(0.05)] = [20000, 6668, 4000, 2000, 1000]
oblidb_large_reading_costs[str(0.05)] = [10000, 3334, 2000, 1000, 500]
oblidb_large_writing_costs[str(0.05)] = [10000, 3334, 2000, 1000, 500]

oblidb_large_all_costs[str(0.1)] = [20000, 6668, 4000, 2000, 1000]
oblidb_large_reading_costs[str(0.1)] = [10000, 3334, 2000, 1000, 500]
oblidb_large_writing_costs[str(0.1)] = [10000, 3334, 2000, 1000, 500]

oblidb_large_all_costs[str(0.2)] = [20000, 6668, 4000, 2000, 1000]
oblidb_large_reading_costs[str(0.2)] = [10000, 3334, 2000, 1000, 500]
oblidb_large_writing_costs[str(0.2)] = [10000, 3334, 2000, 1000, 500]

oblidb_large_all_costs[str(0.3)] = [20000, 6668, 4000, 2000, 1000]
oblidb_large_reading_costs[str(0.3)] = [10000, 3334, 2000, 1000, 500]
oblidb_large_writing_costs[str(0.3)] = [10000, 3334, 2000, 1000, 500]

# ObliDB Hash overall costs
oblidb_hash_all_costs = {}
oblidb_hash_reading_costs = {}
oblidb_hash_writing_costs = {}

oblidb_hash_all_costs[str(0.01)] = [100004, 40003, 20004, 20003, 20003]
oblidb_hash_reading_costs[str(0.01)] = [50004, 20003, 10004, 10003, 10003]
oblidb_hash_writing_costs[str(0.01)] = [50000, 20000, 10000, 10000, 10000]

oblidb_hash_all_costs[str(0.05)] = [100004, 40003, 20004, 20003, 20003]
oblidb_hash_reading_costs[str(0.05)] = [50004, 20003, 10004, 10003, 10003]
oblidb_hash_writing_costs[str(0.05)] = [50000, 20000, 10000, 10000, 10000]

oblidb_hash_all_costs[str(0.1)] = [100004, 40003, 20004, 20003, 20003]
oblidb_hash_reading_costs[str(0.1)] = [50004, 20003, 10004, 10003, 10003]
oblidb_hash_writing_costs[str(0.1)] = [50000, 20000, 10000, 10000, 10000]

oblidb_hash_all_costs[str(0.2)] = [100004, 40003, 20004, 20003, 20003]
oblidb_hash_reading_costs[str(0.2)] = [50004, 20003, 10004, 10003, 10003]
oblidb_hash_writing_costs[str(0.2)] = [50000, 20000, 10000, 10000, 10000]

oblidb_hash_all_costs[str(0.3)] = [100004, 40003, 20004, 20003, 20003]
oblidb_hash_reading_costs[str(0.3)] = [50004, 20003, 10004, 10003, 10003]
oblidb_hash_writing_costs[str(0.3)] = [50000, 20000, 10000, 10000, 10000]



# I have generated the charts based on the following tutorial example:
# https://www.python-graph-gallery.com/11-grouped-barplot
def generate_chart(reading_costs, writing_costs, total_costs, block_size, memory_size):
    algorithms = ['Opaque Filtering', 'ObliDB Small', 'ObliDB Large', 'ObliDB Hash']

    column_width = 0.25

    positions_reading = np.arange(len(reading_costs))
    positions_writing = [previous_width + column_width for previous_width in positions_reading]
    positions_total = [previous_width + column_width for previous_width in positions_writing]

    # I have choose the colours from the following website:
    # https://www.color-hex.com/
    plt.figure(figsize=[10, 7])
    plt.bar(positions_reading, reading_costs,color='#a98d7f', edgecolor='white', width=column_width, label='Reading Operations')
    plt.bar(positions_writing, writing_costs, color='#6a8154', edgecolor='white', width=column_width, label='Writing Operations')
    plt.bar(positions_total, total_costs, color='#714635', edgecolor='white', width=column_width, label='All Operations')

    plt.xlabel('Algorithm', fontsize=20)
    plt.ylabel('Cost [Number of memory accesses]', fontsize=20)
    plt.yscale('log', base=2)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.xticks([r + column_width for r in range(len(positions_reading))], algorithms)
    plt.title('Cost of filtering algorithms (block size =' + str(block_size) + ')', fontsize=25)

    plt.legend(bbox_to_anchor=(0., 1.2, 1., .102), loc='upper left',
           ncol=2, mode="expand", borderaxespad=0., fontsize=20)
    plt.show()

def generate_chart_1(opaque_all, opaque_reading, oblidb_small_all, oblidb_small_reading,
                     oblidb_large_all, oblidb_large_reading, oblidb_hash_all,
                     oblidb_hash_reading, memory_size):
    block_sizes = ['1 record','3 records', '5 records', '10 records', '20 records']

    column_width = 0.2

    positions_1 = np.arange(5)
    positions_2 = [previous_width + column_width for previous_width in positions_1]
    positions_3 = [previous_width + column_width for previous_width in positions_2]
    positions_4 = [previous_width + column_width for previous_width in positions_3]
    positions_5 = [previous_width + column_width for previous_width in positions_4]

    plt.figure(figsize=[10, 7])
    # I have choose the colours from the following website:
    # https://www.color-hex.com/
    plt.bar(positions_1, opaque_all, color='#aa77aa', width=column_width, edgecolor='white', label='Opaque filtering query')
    plt.bar(positions_1, opaque_reading,color='#a7a7a7', width=column_width, edgecolor='white', label='Reading cost')
    plt.bar(positions_2, oblidb_small_all, color='#46282a', edgecolor='white', width=column_width, label='ObliDB Small query')
    plt.bar(positions_2, oblidb_small_reading, color='#a7a7a7', edgecolor='white', width=column_width)
    plt.bar(positions_3, oblidb_large_all, color='#117791', edgecolor='white', width=column_width, label='ObliDB Large query')
    plt.bar(positions_3, oblidb_large_reading, color='#a7a7a7', edgecolor='white', width=column_width)
    plt.bar(positions_4, oblidb_hash_all, color='#99281f', edgecolor='white', width=column_width, label='ObliDB Hash query')
    plt.bar(positions_4, oblidb_hash_reading, color='#a7a7a7', edgecolor='white', width=column_width)

    plt.xlabel('Block size', fontsize=20)
    plt.ylabel('Cost [Number of memory accesses]', fontsize=20)
    plt.yscale('log', base=2)
    plt.yticks(fontsize=20)
    plt.xticks([r + column_width for r in range(len(positions_1))], block_sizes, fontsize=20)
    plt.title('Cost of filtering algorithms (memory size= '+ str(memory_size) + 'MB )', fontsize=20)

    plt.legend()
    plt.show()

def generate_chart_2(oblidb_small_all_costs, oblidb_small_reading):
    #block_sizes = ['1 record','3 records', '5 records', '10 records', '20 records']
    block_size_index = 4
    column_width = 0.5
    oblivious_memory_sizes = ['0.01 MB', '0.05 MB', '0.1 MB', '0.2 MB', '0.3 MB']

    positions_1 = np.arange(5)
    positions_2 = [previous_width + column_width for previous_width in positions_1]
    positions_3 = [previous_width + column_width for previous_width in positions_2]

    all_costs_block_20 = [oblidb_small_all_costs[(str(0.01))][block_size_index],
                    oblidb_small_all_costs[(str(0.05))][block_size_index],
                    oblidb_small_all_costs[(str(0.1))][block_size_index],
                    oblidb_small_all_costs[(str(0.2))][block_size_index],
                    oblidb_small_all_costs[(str(0.3))][block_size_index]]

    reading_costs_block_20 = [oblidb_small_reading[(str(0.01))][block_size_index],
                 oblidb_small_reading[(str(0.05))][block_size_index],
                 oblidb_small_reading[(str(0.1))][block_size_index],
                 oblidb_small_reading[(str(0.2))][block_size_index],
                 oblidb_small_reading[(str(0.3))][block_size_index]]
    block_size_index = 3

    all_costs_block_10 = [oblidb_small_all_costs[(str(0.01))][block_size_index],
                          oblidb_small_all_costs[(str(0.05))][block_size_index],
                          oblidb_small_all_costs[(str(0.1))][block_size_index],
                          oblidb_small_all_costs[(str(0.2))][block_size_index],
                          oblidb_small_all_costs[(str(0.3))][block_size_index]]

    reading_costs_block_10 = [oblidb_small_reading[(str(0.01))][block_size_index],
                              oblidb_small_reading[(str(0.05))][block_size_index],
                              oblidb_small_reading[(str(0.1))][block_size_index],
                              oblidb_small_reading[(str(0.2))][block_size_index],
                              oblidb_small_reading[(str(0.3))][block_size_index]]

    block_size_index = 2

    all_costs_block_5 = [oblidb_small_all_costs[(str(0.01))][block_size_index],
                          oblidb_small_all_costs[(str(0.05))][block_size_index],
                          oblidb_small_all_costs[(str(0.1))][block_size_index],
                          oblidb_small_all_costs[(str(0.2))][block_size_index],
                          oblidb_small_all_costs[(str(0.3))][block_size_index]]

    reading_costs_block_5 = [oblidb_small_reading[(str(0.01))][block_size_index],
                              oblidb_small_reading[(str(0.05))][block_size_index],
                              oblidb_small_reading[(str(0.1))][block_size_index],
                              oblidb_small_reading[(str(0.2))][block_size_index],
                              oblidb_small_reading[(str(0.3))][block_size_index]]

    block_size_index = 1

    all_costs_block_3 = [oblidb_small_all_costs[(str(0.01))][block_size_index],
                          oblidb_small_all_costs[(str(0.05))][block_size_index],
                          oblidb_small_all_costs[(str(0.1))][block_size_index],
                          oblidb_small_all_costs[(str(0.2))][block_size_index],
                          oblidb_small_all_costs[(str(0.3))][block_size_index]]

    reading_costs_block_3 = [oblidb_small_reading[(str(0.01))][block_size_index],
                              oblidb_small_reading[(str(0.05))][block_size_index],
                              oblidb_small_reading[(str(0.1))][block_size_index],
                              oblidb_small_reading[(str(0.2))][block_size_index],
                              oblidb_small_reading[(str(0.3))][block_size_index]]

    block_size_index = 0

    all_costs_block_1 = [oblidb_small_all_costs[(str(0.01))][block_size_index],
                         oblidb_small_all_costs[(str(0.05))][block_size_index],
                         oblidb_small_all_costs[(str(0.1))][block_size_index],
                         oblidb_small_all_costs[(str(0.2))][block_size_index],
                         oblidb_small_all_costs[(str(0.3))][block_size_index]]

    reading_costs_block_1 = [oblidb_small_reading[(str(0.01))][block_size_index],
                             oblidb_small_reading[(str(0.05))][block_size_index],
                             oblidb_small_reading[(str(0.1))][block_size_index],
                             oblidb_small_reading[(str(0.2))][block_size_index],
                             oblidb_small_reading[(str(0.3))][block_size_index]]

    column_width = 0.15

    positions_1 = np.arange(5)
    positions_2 = [previous_width + column_width for previous_width in positions_1]
    positions_3 = [previous_width + column_width for previous_width in positions_2]
    positions_4 = [previous_width + column_width for previous_width in positions_3]
    positions_5 = [previous_width + column_width for previous_width in positions_4]

    plt.figure(figsize=[10, 7])
    # I have choose the colours from the following website:
    # https://www.color-hex.com/
    # https://www.colorhexa.com/16790e
    plt.bar(positions_1, all_costs_block_1, color='white', width=column_width, edgecolor='black',label='Writing cost')
    plt.bar(positions_1, reading_costs_block_1, color='#aa77aa', width=column_width, edgecolor='black', label='Reading cost (1 record/block)')
    plt.bar(positions_2, all_costs_block_3, color='white', edgecolor='black', width=column_width)
    plt.bar(positions_2, reading_costs_block_3, color='#46282a', edgecolor='black', width=column_width,
            label='Reading cost (3 records/block)')
    plt.bar(positions_3, all_costs_block_5, color='white', edgecolor='black', width=column_width)
    plt.bar(positions_3, reading_costs_block_5, color='#117791', edgecolor='black', width=column_width,
            label='Reading cost (5 records/block)')
    plt.bar(positions_4, all_costs_block_10, color='white', edgecolor='black', width=column_width)
    plt.bar(positions_4, reading_costs_block_10, color='#99281f', edgecolor='black', width=column_width,
            label='Reading cost (10 records/block)')
    plt.bar(positions_5, all_costs_block_20, color='white', edgecolor='black', width=column_width)
    plt.bar(positions_5, reading_costs_block_20, color='#bbbb22', edgecolor='black', width=column_width,
            label='Reading cost (20 records/block)')

    plt.xlabel('Oblivious memory size', fontsize=25)
    plt.ylabel('Cost [Number of memory accesses]', fontsize=25)
    plt.yticks(fontsize=20)
    plt.yscale('log', base=2)
    plt.xticks([r + column_width for r in range(len(positions_1))], oblivious_memory_sizes, fontsize=20)
    plt.title('Cost of ObliDB\'s Small filtering algorithm', fontsize=30)

    plt.legend(fontsize=16.9)
    plt.show()

for memory in oblivious_memory_sizes:
    for block_size_index in range(len(block_sizes)):
        print('Memory' ,memory)
        print('block', block_sizes[block_size_index])
        print('Readig: ', oblidb_small_reading_costs[str(memory)][block_size_index])
        print('Writing: ',oblidb_small_writing_costs[str(memory)][block_size_index])
        print('All', oblidb_small_all_costs[str(memory)][block_size_index])
        print('----------------------------------------------------')
        reading_costs = [reading_costs_opaque_filtering[block_size_index], oblidb_small_reading_costs[str(memory)][block_size_index],
                         oblidb_large_reading_costs[str(memory)][block_size_index], oblidb_hash_reading_costs[str(memory)][block_size_index]]
        writing_costs = [writing_costs_opaque_filtering[block_size_index], oblidb_small_writing_costs[str(memory)][block_size_index],
                         oblidb_large_writing_costs[str(memory)][block_size_index], oblidb_hash_writing_costs[str(memory)][block_size_index]]
        all_costs = [all_costs_opaue_filtering[block_size_index], oblidb_small_all_costs[str(memory)][block_size_index],
                         oblidb_large_all_costs[str(memory)][block_size_index], oblidb_hash_all_costs[str(memory)][block_size_index]]
        generate_chart(reading_costs, writing_costs, all_costs, block_sizes[block_size_index], memory)
        '''
    #generate_chart_1(all_costs_opaue_filtering, reading_costs_opaque_filtering, oblidb_small_all_costs[str(memory)],
                     #oblidb_small_reading_costs[str(memory)], oblidb_large_all_costs[str(memory)], oblidb_large_reading_costs[str(memory)],
                     #oblidb_hash_all_costs[str(memory)], oblidb_hash_reading_costs[str(memory)],  memory)'''
generate_chart_2(oblidb_small_all_costs, oblidb_small_reading_costs)
