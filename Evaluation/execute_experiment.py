import os
from database import dataframe_table
from database import database
import pandas as pd
from Evaluation import execute_oblidb_filtering, execute_opaque_filtering_queries
import math

block_sizes = [1, 3, 5, 10, 20]
#table_sizes = [math.floor(artists_table_size/4), math.floor(0.5 * artists_table_size), math.floor(0.75 * artists_table_size), artists_table_size]
oblivious_memory_sizes = [0.01, 0.05, 0.1, 0.2 ,0.3]
for block_size in block_sizes:
    for memory_size in oblivious_memory_sizes:
        execute_opaque_filtering_queries.execute_filtering(block_size=block_size)