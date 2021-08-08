import database.table

class DataBase():
    __tables = {}

    def add_table(self, table, name):
        self.__tables[name] = table

    def get_table(self, name):
        try:
            return self.__tables[name]
        except:
            print('There is no {} table in the database'.format(name))