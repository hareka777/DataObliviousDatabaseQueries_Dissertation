import operator
import pandas as pd
from database import dataframe_table
import numpy as np
from Spark import spark_queries
from database import table
from database import database
from Opaque import opaque_queries as opaque
from database import table
from database import numpy_table
import os


'''cars = {'Brand': ['Honda','Toyota','Ford','Audi', 'Volvo', 'Fiat', 'Mazda', 'Jeep',
                  'Volkswagen', 'Alfa Romeo', 'Dacia', 'Smart', 'Mini', 'Volkswagen', 'Citroen', 'Peugeot'],
        'Model': ['Civic','Corolla','Focus','A4', 'XC90', '500', '3', 'Compass',
                  'Passat', np.nan , 'Duster', np.nan, 'Cooper', 'Polo', 'Cactus', '207'],
        'Price': [22000,25000,27000,35000, 60000, 19000, 24000, 47000, 31000, 35000, 15000, 21000, 57000, 43000, 20000, 11000]
        }

car_df = pd.DataFrame(cars, columns = ['Brand', 'Model', 'Price'])
car_df.to_csv('Cars.csv')

manufacturers = {'Brand': ['Honda','Toyota','Ford','Audi', 'Volvo', 'Fiat', 'Mazda', 'Jeep',
                  'Volkswagen', 'Alfa Romeo', 'Dacia', 'Smart', 'Mini', 'Citroen', 'Peugeot'],
                 'ID': ['Q234', 'A21', 'F08', 'A497', 'V511', 'F053', 'M639', 'J077',
                           'Q342', 'Q657', 'C422', 'V655', 'V00', 'G777', 'A011']}

manufacturer_df = pd.DataFrame(manufacturers, columns = ['Brand', 'ID'])
manufacturer_df.to_csv('Manufacturers.csv')
#manufacturer_table = table.Table().set_table(manufacturer_df)

#car_table = table.Table('Cars.csv')
#manufacturer_table = table.Table('Manufacturers.csv')

#database = database.DataBase()
#database.add_table(car_table, 'Cars')
#database.add_table(manufacturer_table, 'Manufacturers')

# setting up the database

opaque = opaque.Opaque(database)
result = opaque.execute_query(query='select Cars.Price, Manufacturers.ID from Manufacturers join Cars on Manufacturers.Brand=Cars.Brand') #where Nationality == Hungarian and Gender==Female')
print('RESULt')
print(result)'''

t = np.array([[0,1,2,3,4],[5,6,7,8,9],[10,11,12,13,14]]).view(numpy_table.NumpyTable)
print(t)


def transposed(table):
    transposed_table = np.empty((table.shape[1], table.shape[0]), dtype=table.dtype).view(numpy_table.NumpyTable)

    for row in range(table.shape[0]):
        for column in range(table.shape[1]):
            transposed_table[column, row] = table[row, column]
    return transposed_table

'''data_path = os.path.join(os.getcwd(), 'data/csv/'+'Artworks.csv')
table = pd.read_csv(data_path, delimiter=',')
new_table = pd.DataFrame(columns= table.columns)
for row in range(table.shape[0]):
    record = table.loc[row]
    if ',' in str(record.ConstituentID):
        pieces = record.ConstituentID.split(',')
        for piece in pieces:
            new_record = record.copy(deep=True)
            new_record.ConstituentID = piece
            new_record = pd.DataFrame(new_record).transpose()
            new_table = pd.concat([new_table, new_record])
    else:
        rec = pd.DataFrame(record).transpose()
        new_table = pd.concat([new_table, rec])

new_table.to_csv('new_arts.csv')
'''

data_path = os.path.join(os.getcwd(), 'data/csv/'+'Artworks.csv')
table = pd.read_csv(data_path, delimiter=',')
new_table = pd.DataFrame(columns= table.columns)
for row in range(table.shape[0]):
    record = table.loc[row]
    if ',' in str(record.ConstituentID):
        table.drop([row], axis=0, inplace=True)

table.to_csv('new_arts.csv')

#data = dataframe_table.DataFrameTable(data=df)
#print(data.loc[:5])