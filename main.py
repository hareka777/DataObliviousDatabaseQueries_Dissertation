from database import table
from database import database as db
from CommonBuildingBlocks import utils
from Spark import spark_query_executor as spark
from MetricsTracker import cost_tracker
from ObliDB import ObliDB_queries as oblidb
from Opaque import opaque_queries as opaque

#artists_table = dataframe_table.DataFrameTable('Artists.csv')
'''cars_table = utils.read_csv_table('Cars.csv')
manufacturers_table = utils.read_csv_table('Manufacturers.csv')

# setting up database
database = database.DataBase()
database.add_table(cars_table, 'Cars')
database.add_table(manufacturers_table, 'Manufacturers')'''
#car_table = table.Table('Cars.csv')
#manufacturer_table = table.Table('Manufacturers.csv')
#item = manufacturer_table[3]

'''database = database.DataBase()
database.add_table(car_table, 'Cars')
database.add_table(manufacturer_table, 'Manufacturers')

ca = dataframe_table.DataFrameTable(car_table.get_table())
ca = ca.reset_index().set_index(keys=['Brand'])
ca.sort_values(by=['Brand'], inplace=True)
ca = ca.reset_index()
ca = ca.drop(columns= ['index'])
print(ca)'''

'''car_table = utils.read_csv_table('Cars.csv')
manufacturer_table = utils.read_csv_table('Manufacturers.csv')
#item = manufacturer_table[3]

database = database.DataBase()
database.add_table(car_table, 'Cars')
database.add_table(manufacturer_table, 'Manufacturers')'''

# creating the database
artists_table = utils.read_csv_table('Artists.csv')
artworks_table = utils.read_csv_table('Artworks.csv')
database = db.DataBase()
database.add_table(artists_table[0:100], 'Artists')
database.add_table(artworks_table[0:100], 'Artworks')

# setting up Opaque
opaque = opaque.Opaque(database, oblivious_memory_size=0.01)
cost_tracker.ObliviousTracker.set_block_size(3)
'''
#cost_tracker.ObliviousTracker.set_block_size(3)
#result = opaque.execute_query(query='select DisplayName, BeginDate from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID') #where Nationality == Hungarian and Gender==Female')
#result = opaque.execute_query(query='select Cars.Price, Manufacturers.ID from Manufacturers join Cars on Manufacturers.Brand=Cars.Brand')
result = opaque.execute_query(query='select Brand,Model from Cars where Price!=24000')
cost_tracker.ObliviousTracker.query_completed()
result = opaque.execute_query(query='select Brand,Model from Cars where Price>24000')
cost_tracker.ObliviousTracker.query_completed()
result = opaque.execute_query(query='select Brand,Model from Cars where Price<24000')
cost_tracker.ObliviousTracker.query_completed()
result = opaque.execute_query(query='select Brand,Model from Cars where Price!=60000')
cost_tracker.ObliviousTracker.query_completed()
result = opaque.execute_query(query='select Gender, BeginDate from Artists where Nationality==American')
cost_tracker.ObliviousTracker.query_completed()
result = opaque.execute_query(query='select DisplayName, BeginDate, EndDate, ConstituentID, Gender from Artists where EndDate<448')
cost_tracker.ObliviousTracker.query_completed()
#cost_tracker.ObliviousTracker.log_experiment_results('opaque_test')
'''
result = opaque.execute_query(query='select Artists.DisplayName, Artworks.ConstituentID from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Classification!=Drawing')
cost_tracker.ObliviousTracker.query_completed()
result = opaque.execute_query(query='select Artists.DisplayName, Artworks.ConstituentID from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Classification==Photograph')
cost_tracker.ObliviousTracker.query_completed()
cost_tracker.ObliviousTracker.log_experiment_results('opaque_test')


# creating the database
artists_table = utils.read_csv_table('Artists.csv')
artworks_table = utils.read_csv_table('Artworks.csv')
database = db.DataBase()
database.add_table(artists_table[0:100], 'Artists')
database.add_table(artworks_table[0:100], 'Artworks')


# setting up ObliDB
oblidb = oblidb.ObliDB(database, 5)

'''result = oblidb.filtering_SMALL(query='select Brand,Model from Cars where Price==24000')
cost_tracker.ObliviousTracker.query_completed()
result = oblidb.filtering_LARGE(query='select Brand from Manufacturers where ID==F08')
cost_tracker.ObliviousTracker.query_completed()
result = oblidb.filtering_HASH(query='select Brand from Manufacturers where ID==F08')
cost_tracker.ObliviousTracker.query_completed()
result = oblidb.hash_join(query='select Cars.Price, Manufacturers.ID from Manufacturers join Cars on Manufacturers.Brand=Cars.Brand where Cars.Price>30000')
cost_tracker.ObliviousTracker.query_completed()
result = oblidb.sort_merge_join(query='select Cars.Price, Manufacturers.ID from Manufacturers join Cars on Manufacturers.Brand=Cars.Brand where Cars.Price>30000')
cost_tracker.ObliviousTracker.query_completed()
result = oblidb.hash_join(query='select Artists.DisplayName, Artists.Nationality from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artists.Nationality==Hungarian')
cost_tracker.ObliviousTracker.query_completed()
result = oblidb.hash_join(query='select Artists.DisplayName, Artworks.ConstituentID from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Classification!=Collage')
cost_tracker.ObliviousTracker.query_completed()
cost_tracker.ObliviousTracker.log_experiment_results('oblidb_test')'''

# Spark setup
myspark = spark.Processor()
myspark.add_database_table('Artists')
myspark.add_database_table('Artworks')

# Spark experiment
myspark.execute_query_with_metrics("select DisplayName from Artists where ConstituentID=5")
myspark.execute_query_with_metrics("select Artists.DisplayName, Artworks.ConstituentID from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Classification!='Drawing'")
myspark.execute_query_with_metrics("select Artists.DisplayName, Artworks.ConstituentID from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Classification=='Photograph'")
cost_tracker.SparkTracker.log_experiment_results('Spark_Metrics')