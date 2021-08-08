from Evaluation import execute_opaque_join_queries

block_sizes = [1, 3, 5, 10, 20]
#table_sizes = [math.floor(artists_table_size/4), math.floor(0.5 * artists_table_size), math.floor(0.75 * artists_table_size), artists_table_size]
oblivious_memory_sizes = [0.0015, 0.0075, 0.015, 0.03 ,0.045]
for block_size in block_sizes:
    for memory_size in oblivious_memory_sizes:
        execute_opaque_join_queries.execute_join(block_size=block_size, oblivious_memory_size=memory_size)