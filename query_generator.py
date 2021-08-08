from CommonBuildingBlocks import utils
import numpy as np

artists_table = utils.read_csv_table('Artists.csv')
#artists_table = artists_table.sample(n=5000)
#artists_table.to_csv('final_artists_table.csv')

# selecting 20.000 random elements from the Artworks table and we're saving the final table as well
artworks_table = utils.read_csv_table('Artworks.csv')#[0:20000]
#artworks_table = artworks_table.sample(n=5000)
#artworks_table.to_csv('final_artworks_table.csv')

queries_artists_filtering = []

# generating queries for the Artists table filtering queries
columns = artists_table.columns
unique_nationalities = artists_table['Nationality'].unique()
unique_nationalities = np.random.choice(unique_nationalities, 17, replace=False)
unique_genders = artists_table['Gender'].unique()
#unique_genders = np.random.choice(unique_genders, 1, replace=False)

# generating queries where we're filtering for the Nationality column
for i in range(len(unique_nationalities)):
    # randomly select a few columns to for the query's SELECT part

    # selecting how many columns we'll select
    no_columns = np.random.choice(np.arange(1,len(columns)))
    selected_columns = np.random.choice(columns, no_columns, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns:
        selection_string = selection_string + ' ' + column + ','

    # randomly choose between the '==' and '!=' operations
    equal = np.random.choice([0, 1])
    equality = None
    if equal == 0:
        equality = '=='
    else:
        equality = '!='

    # adding the query to the collection
    if type(unique_nationalities[i]) == str:
        queries_artists_filtering.append('select' + selection_string[:-1] + ' from Artists where Nationality' + equality + unique_nationalities[i])

# generating queries where we're filtering for the Gender column
# the method to generate the queries is similar to the way
# we've generated them at the previous query
for i in range(len(unique_genders)):
    # selecting how many columns we'll select
    no_columns = np.random.choice(np.arange(1,len(columns)))
    selected_columns = np.random.choice(columns, no_columns, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns:
        selection_string = selection_string + ' ' + column + ','

    equal = np.random.choice([0, 1])

    equality = None
    if equal == 0:
        equality = '=='
    else:
        equality = '!='

    if type(unique_genders[i]) == str:
        queries_artists_filtering.append(
            'select' + selection_string[:-1] + ' from Artists where Gender' + equality + unique_genders[i])

# generating interval queries based on the BeginDate and EndDate columns

# getting minimum and maximum values for the queries
begin_date_min = artists_table['BeginDate'].min()
begin_date_max = artists_table['BeginDate'].max()

end_date_min = artists_table['EndDate'].min()
end_date_max = artists_table['EndDate'].max()

# generating 20 queries for both the begin and and date filering

for i in range(18):
    # selecting a random year between the smallest and largest year
    random_date = np.random.choice(np.arange(begin_date_min, begin_date_max, 1))
    random_date_end = np.random.choice(np.arange(end_date_min, end_date_max, 1))

    # choosing an operator between < and >
    operator_choice_begin = np.random.choice([0,1])
    operator_choice_end = np.random.choice([0, 1])

    # generating the SELECT section
    no_columns_begin = np.random.choice(np.arange(1, len(columns)))
    selected_columns_begin = np.random.choice(columns, no_columns_begin, replace=False)

    no_columns_end = np.random.choice(np.arange(1, len(columns)))
    selected_columns_end = np.random.choice(columns, no_columns_end, replace=False)

    selection_string_begin = ''
    selection_string_end = ''

    # created comma separated string from the selected columns
    for column in selected_columns_begin:
        selection_string_begin = selection_string_begin + ' ' + column + ','

    for column in selected_columns_end:
        selection_string_end = selection_string_end + ' ' + column + ','

    operator_begin = None
    operator_end = None
    if operator_choice_begin == 0:
        operator_begin = '>'
    else:
        operator_begin = '<'

    if operator_choice_end == 0:
        operator_end = '>'
    else:
        operator_end = '<'

    queries_artists_filtering.append('select' + selection_string_begin[:-1] + ' from Artists where BeginDate' + operator_begin + str(random_date))

    queries_artists_filtering.append(
        'select' + selection_string_begin[:-1] + ' from Artists where EndDate' + operator_end + str(random_date_end))

# writing the queries to a file

'''file = open("generated_queries/artists_filtering_queries.txt", "w")
for query in queries_artists_filtering:
    file.write(query + "\n")
file.close()

queries_artworks_filtering = []'''
# generating filtering queries for the Artworks table
columns = artworks_table.columns

# generating queries for the width, height columns
width_min = artworks_table['Width'].min()
width_max = artworks_table['Width'].max()

height_min = artworks_table['Height'].min()
height_max = artworks_table['Height'].max()

for i in range(10):
    # selecting a random year between the smallest and largest year
    height = np.random.choice(np.arange(height_min, height_max, 1))
    width = np.random.choice(np.arange(width_min, width_max, 1))

    # choosing an operator between < and >
    operator_choice_height = np.random.choice([0,1])
    operator_choice_width = np.random.choice([0, 1])

    # generating the SELECT section
    no_columns_height = np.random.choice(np.arange(1, len(columns)))
    selected_columns_height = np.random.choice(columns, no_columns_height, replace=False)

    no_columns_width = np.random.choice(np.arange(1, len(columns)))
    selected_columns_width = np.random.choice(columns, no_columns_width, replace=False)

    selection_string_height = ''
    selection_string_width = ''

    # created comma separated string from the selected columns
    for column in selected_columns_height:
        selection_string_height = selection_string_height + ' ' + column + ','

    for column in selected_columns_width:
        selection_string_width = selection_string_width + ' ' + column + ','

    operator_height = None
    operator_width = None
    if operator_choice_height == 0:
        operator_height = '>'
    else:
        operator_height = '<'

    if operator_choice_width == 0:
        operator_width = '>'
    else:
        operator_width = '<'

    queries_artists_filtering.append('select' + selection_string_height[:-1] + ' from Artworks where Height' + operator_height + str(int(height)))

    queries_artists_filtering.append(
        'select' + selection_string_width[:-1] + ' from Artworks where Width' + operator_width + str(int(width)))


# generating queries based on the Cataloged and Classification column values

unique_cataloged = artworks_table['Cataloged'].unique()
#unique_cataloged = np.random.choice(unique_cataloged, 1, replace=False)
unique_classification = artworks_table['Classification'].unique()
#unique_classification = np.random.choice(unique_classification, 1, replace=False)
#unique_classification = np.random.choice(unique_classification, 40, replace=False)

# Cataloged filter
for i in range(len(unique_cataloged)):
    # randomly select a few columns to for the query's SELECT part

    # selecting how many columns we'll select
    no_columns = np.random.choice(np.arange(1,len(columns)))
    selected_columns = np.random.choice(columns, no_columns, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns:
        selection_string = selection_string + ' ' + column + ','

    # adding the query to the collection
    if type(unique_cataloged[i]) == str:
        queries_artists_filtering.append('select' + selection_string[:-1] + ' from Artworks where Cataloged' + '==' + unique_cataloged[i])


#  filter for the Classification column
for i in range(len(unique_classification)):
    # randomly select a few columns to for the query's SELECT part

    # selecting how many columns we'll select
    no_columns = np.random.choice(np.arange(1,len(columns)))
    selected_columns = np.random.choice(columns, no_columns, replace=False)

    selection_string = ''
    # created comma separated string from the selected columns
    for column in selected_columns:
        selection_string = selection_string + ' ' + column + ','

    operator_choice = np.random.choice([0,1])
    operator = None
    if operator_choice == 0:
        operator = '=='
    else:
        operator = '!='
    # adding the query to the collection
    if type(unique_classification[i]) == str:
        queries_artists_filtering.append('select' + selection_string[:-1] + ' from Artworks where Classification' + operator + unique_classification[i])


# writing the queries to a file
file = open("generated_queries/filtering_queries_100.txt", "w")
for query in queries_artists_filtering:
    file.write(query + "\n")
file.close()