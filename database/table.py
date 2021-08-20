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

    def __getitem__(self, item):
        print('Getted')
        return self.__table.loc[item]

    def __setitem__(self, item, data):
        print('Setted')
        self.__table[item] = data

    def get_table(self):
        return self.__table