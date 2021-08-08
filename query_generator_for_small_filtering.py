from CommonBuildingBlocks import utils
import numpy as np

artists_table = utils.read_csv_table('Artists.csv')

queries_artists_filtering = []

# generating queries for the Artists table filtering queries
columns = artists_table.columns
unique_nationalities = artists_table['Nationality'].unique()
unique_nationalities = np.random.choice(unique_nationalities, 25, replace=False)

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

    # adding the query to the collection
    if type(unique_nationalities[i]) == str:
        queries_artists_filtering.append('select' + selection_string[:-1] + ' from Artists where Nationality==' + unique_nationalities[i])

file = open("generated_queries/artists_filtering_queries_small.txt", "w")
for query in queries_artists_filtering:
    file.write(query + "\n")
file.close()


artworks_table = utils.read_csv_table('final_artworks_table.csv')
unique_classification = artworks_table['Classification'].unique()
unique_classification = np.random.choice(unique_classification, 25, replace=False)
queries = []
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
        queries.append('select' + selection_string[:-1] + ' from Artworks where Classification' + operator + unique_classification[i])

file = open("generated_queries/artworks_filtering_queries_small.txt", "w")
for query in queries:
    file.write(query + "\n")
file.close()

