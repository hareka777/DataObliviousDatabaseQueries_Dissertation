from database import database, dataframe_table
import os
import pandas as pd
from Opaque import opaque_queries
from MetricsTracker import cost_tracker
from database import database as db

def execute_filtering(block_size):
    print('Block size: ', block_size)
    # settings: setting up the database
    parent_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    artists_table_path = os.path.join(parent_path, 'data/csv/' + 'final_artists_table.csv')
    artworks_table_path = os.path.join(parent_path, 'data/csv/' + 'final_artworks_table.csv')

    artists_table = dataframe_table.DataFrameTable(pd.read_csv(artists_table_path))
    artworks_table = dataframe_table.DataFrameTable(pd.read_csv(artworks_table_path))
    database = db.DataBase()
    database.add_table(artists_table, 'Artists')
    database.add_table(artworks_table, 'Artworks')

    cost_tracker.ObliviousTracker.set_block_size(block_size)
    # settings: importing the queries to a list
    query_list = []
    with open(parent_path + "/generated_queries/final_filtering_queries.txt", "r") as artist_queries:
        query_list = artist_queries.readlines()

    cost_tracker.ObliviousTracker.set_block_size(block_size)
    experiment(database, query_list, oblivious_memory_size=0, block_size=block_size)

def experiment(database, query_list, oblivious_memory_size, block_size):
    opaque = opaque_queries.Opaque(database, oblivious_memory_size=oblivious_memory_size)
    for query in query_list:
        query = query.strip('\n')
        opaque.execute_query(query=query)
        cost_tracker.ObliviousTracker.query_completed()
        print(query)
    cost_tracker.ObliviousTracker.log_experiment_results('Results\Opaque\Filtering\opaque_filtering_block_' +str(block_size) + '.txt ')

    #experiment(query_list, oblivious_memory_size)

