from CommonBuildingBlocks import utils
import numpy as np

# loading the database tables
artists_table = utils.read_csv_table('Artists.csv')[0:300]
artworks_table = utils.read_csv_table('Artworks.csv')[0:300]

queries = []

# generating queries for the Artists table filtering queries
columns_artist = artists_table.columns
columns_artworks = artworks_table.columns
unique_nationalities = artists_table['Nationality'].unique()
#choosing 50 nationlities
unique_nationalities = np.random.choice(unique_nationalities, 1, replace = False)
unique_genders = artists_table['Gender'].unique()
unique_genders = np.random.choice(unique_genders, 1, replace = False)

# generating queries where we're filtering for the Nationality column
for i in range(len(unique_nationalities)):
    # randomly select a few columns to for the query's SELECT part

    # selecting how many columns we'll select from the Artist table
    no_columns_artists = np.random.choice(np.arange(1,len(columns_artist)))
    selected_columns_artists = np.random.choice(columns_artist, no_columns_artists, replace=False)

    # selecting how many columns we'll select from the Artworks table
    no_columns_artworks = np.random.choice(np.arange(1, len(columns_artworks)))
    selected_columns_artworks = np.random.choice(columns_artworks, no_columns_artworks, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns_artists:
        selection_string = selection_string + ' Artists.' + column + ','

    for column in selected_columns_artworks:
        selection_string = selection_string + ' Artworks.' + column + ','

    # randomly choose between the '==' and '!=' operations
    equal = np.random.choice([0, 1])
    equality = None
    if equal == 0:
        equality = '=='
    else:
        equality = '!='

    # adding the query to the collection
    if type(unique_nationalities[i]) == str:
        queries.append('select' + selection_string[:-1] + ' from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artists.Nationality' + equality + unique_nationalities[i])

# generating queries where we're filtering for the Gender column
# the method to generate the queries is similar to the way
# we've generated them at the previous query
for i in range(len(unique_genders)):
    # selecting how many columns we'll select from the Artist table
    no_columns_artists = np.random.choice(np.arange(1, len(columns_artist)))
    selected_columns_artists = np.random.choice(columns_artist, no_columns_artists, replace=False)

    # selecting how many columns we'll select from the Artworks table
    no_columns_artworks = np.random.choice(np.arange(1, len(columns_artworks)))
    selected_columns_artworks = np.random.choice(columns_artworks, no_columns_artworks, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns_artists:
        selection_string = selection_string + ' Artists.' + column + ','

    for column in selected_columns_artworks:
        selection_string = selection_string + ' Artworks.' + column + ','

    equal = np.random.choice([0, 1])

    equality = None
    if equal == 0:
        equality = '=='
    else:
        equality = '!='

    if type(unique_genders[i]) == str:
        queries.append(
            'select' + selection_string[:-1] + ' from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artists.Gender' + equality + unique_genders[i])

# generating interval queries based on the BeginDate and EndDate columns

# getting minimum and maximum values for the queries
begin_date_min = artists_table['BeginDate'].min()
begin_date_max = artists_table['BeginDate'].max()

end_date_min = artists_table['EndDate'].min()
end_date_max = artists_table['EndDate'].max()

# generating 20 queries for both the begin and and date filtering

for i in range(1):
    # selecting a random year between the smallest and largest year
    random_date = np.random.choice(np.arange(begin_date_min, begin_date_max, 1))

    # choosing an operator between < and >
    operator_choice_begin = np.random.choice([0,1])

    # selecting how many columns we'll select from the Artist table
    no_columns_artists = np.random.choice(np.arange(1, len(columns_artist)))
    selected_columns_artists = np.random.choice(columns_artist, no_columns_artists, replace=False)

    # selecting how many columns we'll select from the Artworks table
    no_columns_artworks = np.random.choice(np.arange(1, len(columns_artworks)))
    selected_columns_artworks = np.random.choice(columns_artworks, no_columns_artworks, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns_artists:
        selection_string = selection_string + ' Artists.' + column + ','

    for column in selected_columns_artworks:
        selection_string = selection_string + ' Artworks.' + column + ','

    operator_begin = None
    if operator_choice_begin == 0:
        operator_begin = '>'
    else:
        operator_begin = '<'

    queries.append('select' + selection_string[:-1] + ' from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artists.BeginDate' + operator_begin + str(random_date))

# writing the queries to a file

'''file = open("generated_queries/artists_filtering_queries.txt", "w")
for query in queries_artists_filtering:
    file.write(query + "\n")
file.close()'''

# generating filtering queries for the Artworks table
columns = artworks_table.columns

# generating queries for the width, height columns
width_min = artworks_table['Width'].min()
width_max = artworks_table['Width'].max()

height_min = artworks_table['Height'].min()
height_max = artworks_table['Height'].max()

for i in range(1):
    # selecting a random height
    height = np.random.choice(np.arange(height_min, height_max, 1))

    # choosing an operator between < and >
    operator_choice_height = np.random.choice([0,1])

    # selecting how many columns we'll select from the Artist table
    no_columns_artists = np.random.choice(np.arange(1, len(columns_artist)))
    selected_columns_artists = np.random.choice(columns_artist, no_columns_artists, replace=False)

    # selecting how many columns we'll select from the Artworks table
    no_columns_artworks = np.random.choice(np.arange(1, len(columns_artworks)))
    selected_columns_artworks = np.random.choice(columns_artworks, no_columns_artworks, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns_artists:
        selection_string = selection_string + ' Artists.' + column + ','

    for column in selected_columns_artworks:
        selection_string = selection_string + ' Artworks.' + column + ','

    operator_height = None
    if operator_choice_height == 0:
        operator_height = '>'
    else:
        operator_height = '<'

    queries.append('select' + selection_string[:-1] + ' from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Height' + operator_height + str(int(height)))


# generating queries based on the Cataloged and Classification column values

unique_cataloged = artworks_table['Cataloged'].unique()
unique_cataloged = np.random.choice(unique_cataloged, 1, replace = False)
unique_classification = artworks_table['Classification'].unique()
unique_classification = np.random.choice(unique_classification, 1, replace = False)

# Cataloged filter
for i in range(len(unique_cataloged)):
    # randomly select a few columns to for the query's SELECT part

    # selecting how many columns we'll select from the Artist table
    no_columns_artists = np.random.choice(np.arange(1, len(columns_artist)))
    selected_columns_artists = np.random.choice(columns_artist, no_columns_artists, replace=False)

    # selecting how many columns we'll select from the Artworks table
    no_columns_artworks = np.random.choice(np.arange(1, len(columns_artworks)))
    selected_columns_artworks = np.random.choice(columns_artworks, no_columns_artworks, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns_artists:
        selection_string = selection_string + ' Artists.' + column + ','

    for column in selected_columns_artworks:
        selection_string = selection_string + ' Artworks.' + column + ','

    # adding the query to the collection
    if type(unique_cataloged[i]) == str:
        queries.append('select' + selection_string[:-1] + ' from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Cataloged' + '==' + unique_cataloged[i])

#  Classification filter
for i in range(len(unique_classification)):
    # randomly select a few columns to for the query's SELECT part

    # selecting how many columns we'll select from the Artist table
    no_columns_artists = np.random.choice(np.arange(1, len(columns_artist)))
    selected_columns_artists = np.random.choice(columns_artist, no_columns_artists, replace=False)

    # selecting how many columns we'll select from the Artworks table
    no_columns_artworks = np.random.choice(np.arange(1, len(columns_artworks)))
    selected_columns_artworks = np.random.choice(columns_artworks, no_columns_artworks, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns_artists:
        selection_string = selection_string + ' Artists.' + column + ','

    for column in selected_columns_artworks:
        selection_string = selection_string + ' Artworks.' + column + ','

    operator_choice = np.random.choice([0,1])
    operator = None
    if operator_choice == 0:
        operator = '=='
    else:
        operator = '!='
    # adding the query to the collection
    if type(unique_classification[i]) == str:
        queries.append('select' + selection_string[:-1] + ' from Artists join Artworks on Artists.ConstituentID=Artworks.ConstituentID where Artworks.Classification' + operator + unique_classification[i])


# writing the queries to a file
file = open("generated_queries/join_queries_five.txt", "w")
for query in queries:
    file.write(query + "\n")
file.close()