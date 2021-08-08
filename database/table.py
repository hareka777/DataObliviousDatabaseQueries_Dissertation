import pandas as pd
import os

class Table:

    __table = None
    def __init__(self, file_name, table=None):
        if file_name is not None:
            data_path = os.path.join(os.getcwd(), 'data/csv/'+file_name)
            self.__table = pd.read_csv(data_path, delimiter=',')
        if table is not None:
            self.__table = table

    def set_table(self, table):
        self.__table = table

    '''def get_elements(self, row= None, column=None):
        if row is None:
            return self.__table[column]
        elif column is None:
            return self.__table.loc[row]
        else:
            return self.__table.loc[row][column]

    def get_dimensions(self):
        return self.__table.shape

    def add_column(self, column_name, value):
        self.__table[column_name] = value

    def set_cell_value(self, row_idx, column, value):
        self.__table.at[row_idx , column] = value

    def drop_column(self, column):
        self.__table.drop(column)

    def get_columns(self):
        return self.__table.columns

    def get_slice(self, from_idx, to_idx):
        return self.__table[: to_idx]'''

    def __getitem__(self, item):
        print('Getted')
        return self.__table.loc[item]

    def __setitem__(self, item, data):
        print('Setted')
        self.__table[item] = data

    def get_table(self):
        return self.__table