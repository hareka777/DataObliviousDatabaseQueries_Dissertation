import Opaque.building_blocks as blocks
import database.query_parser as parser
import pandas as pd
import numpy as np
from database import dataframe_table
from MetricsTracker import cost_tracker
import time
from CommonBuildingBlocks import common_building_blocks

class Opaque:

    __opaque_blocks = None
    __database = None
    __condition_column = 'Condition'
    __oblivious_memory_size = None
    __query = None

    def __init__(self, database, oblivious_memory_size):
        self.__opaque_blocks = blocks.BuildingBlocks(oblivious_memory_size=oblivious_memory_size)
        self.__database = database

    def execute_query(self, query):
        query = parser.QueryParser().parse(query)
        self.__query = query
        if 'join_fields' in query:
            return self.__execute_join(query)
        else:
            return self.__execute_filtering(query)

    def __execute_filtering(self, query):
        table = query['from']
        if len(table) != 1:
            raise ValueError('Only 1 table allowed at filtering queries!')
        table = self.__database.get_table(table[0])

        size_of_table = table.memory_usage(deep=True).sum() * 10 ** -6
        print('Size of table: ', size_of_table)

        if 'where' in query:
            conditions = query['where']
        else:
            conditions = None

        selected_fields = query['select']
        result_table = pd.DataFrame(columns=selected_fields)
        if conditions is None:
            try:
                result_table = table[selected_fields]
            except:
                print('Query is not successful, please try again!')
        else:
            start_time = time.time()
            # executing oblivious filtering
            left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)

            table = self.__scan_table(table, left_conditions, right_conditions, comparators)

            # ending the timer and registering the time elapsed
            end_time = time.time()
            time_elapsed = end_time - start_time
            cost_tracker.ObliviousTracker.register_execution_time(time_elapsed)

        print(table[selected_fields])
        cost_tracker.ObliviousTracker.register_result_table_size(result_table.shape[0])
        return result_table

    def __execute_join(self, query):

        # getting the two database tables from the database
        no_tables = len(query['from'])
        if no_tables != 2:
            raise ValueError('Only 1 table allowed at filtering queries!') # 2 tables are needed for the join query
        tables = []
        for i in range(no_tables):
            tables.append(self.__database.get_table(query['from'][i]))

        # columns to join on
        join_columns = query['join_fields']

        # selected fields
        selected_fields = query['select']

        left_conditions, right_conditions, comparators = None, None, None
        # where conditions
        if 'where' in query:
            conditions = query['where']
            left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)
        else:
            conditions = None

        # starting the timer that measures the execution time
        start = time.time()

        # STAGE 1: union
        unioned_table = dataframe_table.DataFrameTable(pd.concat(tables, ignore_index=True))

        '''# pandas sometimes changes the type of the columns, so we need to make sure it does not happen

        unioned_table = unioned_table.fillna(0)
        for column in tables[0].columns:
            type = tables[0].dtypes[column]
            unioned_table.astype({column: type})

        for column in tables[1].columns:
            type = tables[1].dtypes[column]
            unioned_table.astype({column: type})'''


        unioned_table = self.__opaque_blocks.column_sort(unioned_table, join_columns[0])
        unioned_table.to_csv('column_sort_test.csv')

        # join them together to the unioned table
        #unioned_table = table.Table(None, pd.concat([Tp_sorted.get_table(), Tf_sorted.get_table()], ignore_index=True))

        # STAGE 2
        # partitions scanning
        partitions = self.__opaque_blocks.split_data_to_partitions(unioned_table)
        boundary_records = common_building_blocks.produce_boundary_records(partitions, join_columns[0])

        # STAGE 3
        # boundary procssing step
        processed_boundaries = common_building_blocks.boundary_record_processing(boundary_records)

        # STAGE 4
        # execute joins
        result = common_building_blocks.scan_join(partitions, processed_boundaries, join_columns[0], left_conditions, right_conditions, comparators, query=query)

        # STAGE
        # filtering out non values, filtering the table based on join attributes
        #result = self.__opaque_blocks.column_sort(result, join_columns[0]) #, padding=True)

        # registering the cost elapsed during the join execution
        end = time.time()
        cost_tracker.ObliviousTracker.register_execution_time(end - start)
        cost_tracker.ObliviousTracker.register_result_table_size(result.shape[0])
        result.to_csv('join_result.csv')
        print(result)

    def __scan_table(self, table, left_conditions, right_conditions, comparators):
        table = pd.DataFrame(table)
        result_table = pd.DataFrame(columns=table.columns)
        #table[self.__condition_column] = 1
        results = []
        #row_idx = 0
        for row_idx in range(int(table.shape[0])):

            '''cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
            if row == table.shape[0]:
                row = table[row:]
            else:
                row = table[row : row + 1]
            cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)'''
            row = table.loc[row_idx]
            common_building_blocks.log_metrics(row_idx, read=True)

            for index in range(len(left_conditions)):
                left_condition = left_conditions[index]
                right_condition = right_conditions[index]
                comparator = comparators[index]

                if common_building_blocks.judge_condition(row, left_condition, right_condition, comparator):
                    if index == len(left_conditions) - 1:
                        #table.at[row_idx, self.__condition_column] = 0
                        result_table = result_table.append(row)
                        common_building_blocks.log_metrics(row_idx, read=False)
                else:
                    result_table = result_table.append(pd.Series(np.nan * table.shape[1]), ignore_index=True)
                    common_building_blocks.log_metrics(row_idx, read=False)
            row_idx += 1
        return result_table

    def __scan_sorted_conditioned_table(self, table):
        for index in range(table.shape[0]):
            row = table.loc[index]
            if row[self.__condition_column] == 1:
                return index