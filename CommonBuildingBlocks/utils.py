import os
import pandas as pd
from database import dataframe_table

def read_csv_table(file_name):
    data_path = os.path.join(os.getcwd(), 'data/csv/' + file_name)
    return dataframe_table.DataFrameTable(pd.read_csv(data_path, delimiter=','))