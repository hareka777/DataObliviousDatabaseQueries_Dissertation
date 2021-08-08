import pandas as pd
import database.query_parser as parser
import time
from MetricsTracker import cost_tracker
from database import dataframe_table
import ObliDB.building_blocks as blocks
import Opaque.building_blocks as opaque_blocks
import numpy as np
from math import floor, ceil
from CommonBuildingBlocks import common_building_blocks

class ObliDB():
    __database = None
    __obliDB_blocks = None
    __opaque_blocks = None
    __oblivious_memory_size = None
    __query = None

    def __init__(self, database, oblivious_memory_size):
        self.__database = database
        self.__oblivious_memory_size = oblivious_memory_size
        self.__obliDB_blocks = blocks.BuildingBlocks(oblivious_memory_size)
        self.__opaque_blocks = opaque_blocks.BuildingBlocks()

    def filtering_SMALL(self, query):
        query = parser.QueryParser().parse(query)

        # getting the parameters needed from the query
        table = query['from']
        if len(table) != 1:
            raise ValueError('Only 1 table allowed at filtering queries!')
        table = self.__database.get_table(table[0])

        if 'where' in query:
            conditions = query['where']
        else:
            conditions = None

        left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)

        selected_fields = query['select']

        # starting the timer
        start = time.time()

        temporary_result_table = dataframe_table.DataFrameTable(columns=selected_fields)
        result_table = dataframe_table.DataFrameTable(columns=selected_fields)

        # computing how many result records can fit to the oblivious memory [MB]
        size_of_table = table.memory_usage(deep=True).sum() * 10 ** -6
        oblivious_memory_size = self.__oblivious_memory_size
        number_of_records = table.shape[0]
        # we need to store the current read in record and the buffer of the result records
        records_fit_into_the_enclave = floor(number_of_records * (oblivious_memory_size / size_of_table)) #- 1

        pointer = 0
        number_of_records_in_temporary_result_table = 0
        buffer_full_flag = False

        while pointer < number_of_records - 1:
            buffer_full_flag = False
            for i in range(number_of_records):
                current_record = table.loc[i]
                # logging the accessed memory
                common_building_blocks.log_metrics(i, read=True)

                if i > pointer and buffer_full_flag == False:
                    for index in range(len(left_conditions)):
                        left_condition = left_conditions[index]
                        right_condition = right_conditions[index]
                        comparator = comparators[index]
                        if common_building_blocks.judge_condition(current_record, left_condition, right_condition, comparator):
                            if index == len(left_conditions) - 1:
                                current_record = current_record.to_frame().transpose()[selected_fields]
                                temporary_result_table = dataframe_table.DataFrameTable(pd.concat([temporary_result_table,current_record], ignore_index=True))
                                number_of_records_in_temporary_result_table += 1
                        else:
                            break

                    # when the buffer is full, we copy the records to the return table
                    if number_of_records_in_temporary_result_table == records_fit_into_the_enclave or (i == number_of_records - 1):
                        for temporary_record_index in range(temporary_result_table.shape[0]):
                            temporary_record = temporary_result_table.loc[temporary_record_index].to_frame().transpose()
                            result_table = dataframe_table.DataFrameTable(pd.concat([result_table, temporary_record], ignore_index=True))
                            common_building_blocks.log_metrics(temporary_record_index, read=False)

                        # clearing the buffer
                        temporary_result_table = dataframe_table.DataFrameTable(columns=selected_fields)
                        number_of_records_in_temporary_result_table = 0
                        buffer_full_flag =True

                    pointer = i

        # stopping timer
        stop = time.time()

        cost_tracker.ObliviousTracker.register_execution_time(stop - start)
        cost_tracker.ObliviousTracker.register_result_table_size(result_table.shape[0])
        print(result_table)

    def filtering_LARGE(self, query):
        query = parser.QueryParser().parse(query)

        # getting the parameters needed from the query
        table = query['from']
        if len(table) != 1:
            raise ValueError('Only 1 table allowed at filtering queries!')
        table = self.__database.get_table(table[0])

        if 'where' in query:
            conditions = query['where']
        else:
            conditions = None

        left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)

        selected_fields = query['select']

        # starting the timer
        start = time.time()

        # Step 1: Creating a copy of the table
        result_table = dataframe_table.DataFrameTable(table.copy(deep=True))

        # logging the memory accesses and costs of the copy operation above
        for i in range(result_table.shape[0]):
            # to be able to copy, first, we need to read in the blocks
            common_building_blocks.log_metrics(i, read=True)

            # next, we need to copy and write the records to the result_table
            common_building_blocks.log_metrics(i, read=False)

        for i in range(result_table.shape[0]):
            record = result_table.loc[i]
            # to be able to copy, first, we need to read in the blocks
            common_building_blocks.log_metrics(i, read=True)

            # ckecking the where conditions
            for index in range(len(left_conditions)):
                left_condition = left_conditions[index]
                right_condition = right_conditions[index]
                comparator = comparators[index]
                if common_building_blocks.judge_condition(record, left_condition, right_condition, comparator):
                    if index == len(left_conditions) - 1:
                        # if this is a match, we keep it in the result table, so we're doing a dummy write
                        result_table.at[i] = record
                        common_building_blocks.log_metrics(i, read=False)
                else:
                    result_table.at[i] = np.nan
                    common_building_blocks.log_metrics(i, read=False)
                    break

        # final step: only keep columns which is selected in the query
        result_table = result_table[selected_fields]

        end = time.time()
        cost_tracker.ObliviousTracker.register_execution_time(end - start)
        cost_tracker.ObliviousTracker.register_result_table_size(result_table.shape[0])
        print('LARGE')
        print(result_table)

    def filtering_HASH(self, query):
        query = parser.QueryParser().parse(query)

        # setting the bucket size
        bucket_size = 5

        # getting the parameters of the query: tables, conditions etc.
        table = query['from']
        if len(table) != 1:
            raise ValueError('Only 1 table allowed at filtering queries!')
        table = self.__database.get_table(table[0])

        if 'where' in query:
            conditions = query['where']
        else:
            conditions = None

        left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)

        selected_fields = query['select']

        # starting the timer
        start = time.time()

        # calculating result table size
        result_table_size = ceil(table.shape[0] / 5)
        result_table = {}

        # creating an empty result table: a dictionary that contains 5 'buckets' as a dataframe
        for j in range(result_table_size):
            result_table[j] = pd.DataFrame(index=np.arange(0,5,1), columns=table.columns)

        # we iterate through the table's rows
        for i in range(table.shape[0]):
            record = table.loc[i]
            # logging the accessed element
            common_building_blocks.log_metrics(i, read=True)

            # finding the possible places where we can insert the record if it's a match
            index1, index2 = self.__double_hash_function(i, result_table_size)

            # indicate if we could insert our record
            already_inserted_flag = False

            # checking the where conditions
            for index in range(len(left_conditions)):
                left_condition = left_conditions[index]
                right_condition = right_conditions[index]
                comparator = comparators[index]
                if common_building_blocks.judge_condition(record, left_condition, right_condition, comparator):
                    if index == len(left_conditions) - 1:
                        # if this is a match, we should insert it to the result table

                        for hash_index in [index1, index2]:
                            for bucket_index in range(bucket_size):
                                if result_table[hash_index].loc[bucket_index].any() == False and already_inserted_flag == False:
                                    # logging that we read in the record
                                    common_building_blocks.log_metrics(bucket_index, read=True)

                                    result_table[hash_index].at[bucket_index] = record
                                    # logging we made a write operation
                                    common_building_blocks.log_metrics(bucket_index, read=False)
                                    # indicating that we already inserted our element
                                    already_inserted_flag = True
                                else:
                                    # we make a dummy write
                                    # logging that we read in the record
                                    common_building_blocks.log_metrics(bucket_index, read=True)

                                    result_table[hash_index].at[bucket_index] = result_table[hash_index].loc[bucket_index]
                                    # logging we made a write operation
                                    common_building_blocks.log_metrics(bucket_index, read=False)

                else:
                    for hash_index in [index1, index2]:
                        for bucket_index in range(bucket_size):
                            # logging that we read in the record
                            common_building_blocks.log_metrics(bucket_index, read=True)

                            # we make a dummy write
                            result_table[hash_index].at[bucket_index] = result_table[hash_index].loc[bucket_index]
                            # logging we made a write operation
                            common_building_blocks.log_metrics(bucket_index, read=False)
                    break

        # final step: only keep columns which is selected in the query
        result_table = pd.concat(result_table.values(), ignore_index=True)
        result_table = result_table[selected_fields]

        end = time.time()
        cost_tracker.ObliviousTracker.register_execution_time(end - start)
        cost_tracker.ObliviousTracker.register_result_table_size(result_table.shape[0])

        print('Hash')
        print(result_table)

    def hash_join(self, query):
        query = parser.QueryParser().parse(query)
        self.__query = query

        no_tables = len(query['from'])
        if no_tables != 2:
            raise ValueError('Only 1 table allowed at filtering queries!')  # 2 tables are needed for the join query
        tables = []
        for i in range(no_tables):
            tables.append(self.__database.get_table(query['from'][i]))

        # columns to join on
        join_columns = query['join_fields']
        key = join_columns[0]

        # selected columns
        selected_columns = {}
        for column in query['select']:
            parts = column.split('.')
            table = parts[0]
            column = parts[1]

            if table in selected_columns.keys():
                selected_columns[table].append(column)
            else:
                selected_columns[table] = [column]

        conditions = query['where']
        left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)

        start = time.time()
        oblivious_memory_size = self.__oblivious_memory_size

        # the primary key table to be hashed to keep obliviousness
        # therefore the hash buckets may contain more elements
        T_f = tables[1] # foreign key table
        T_p = tables[0] # primary key table

        # computing how many records fit into the oblivious memory
        size_of_p_f = T_p.memory_usage(deep=True).sum() * 10 ** -6
        number_of_records_T_p = T_p.shape[0]

        # computing the number of records fit into the oblivious memory
        # in addition to the records, as we iterate over the foreign key table later,
        # we need to be able to store this record as well
        number_of_records_fit = floor(number_of_records_T_p * (oblivious_memory_size/size_of_p_f)) # - 1

        if number_of_records_fit > number_of_records_T_p:
            number_of_records_fit = number_of_records_T_p

        # creating a hash table, represented as a dictionary
        hash_table = {}

        # creating the result table
        result_table = dataframe_table.DataFrameTable(columns=query['select'])

        for j in range(ceil(number_of_records_T_p/number_of_records_fit)):
            starting_record = number_of_records_fit * j
            ending_record = (j + 1) * number_of_records_fit
            if ending_record > number_of_records_T_p:
                ending_record = number_of_records_T_p
            # reading in the records from T_p to create a hash table
            for i in range(starting_record, ending_record):

                record = T_p.loc[i]
                common_building_blocks.log_metrics(i, True) # registering the memory access pattern and the cost of reading

                # handling None elements
                if pd.isnull(record[key]):
                    record_key = 'Nan'
                else:
                    record_key = str(record[key])

                if record_key in hash_table.keys():
                    hash_table[record_key].append(record)
                else:
                    hash_table[record_key] = [record]

            # iterating through the foreign key(T_f) table to create joins
            for i in range(T_f.shape[0]):
                record = T_f.loc[i]
                common_building_blocks.log_metrics(i, True)  # registering the memory access pattern and the cost of writing

                if pd.isnull(record[key]):
                    key_to_match = 'Nan'
                else:
                    key_to_match = str(int(record[key]))


                # try to find a match
                if key_to_match in hash_table:
                    # if there is a match and the where conditions are apply, we execute join and write it to the result table
                    record_to_join = hash_table[key_to_match][0][selected_columns[query['from'][0]]]
                    for index in range(len(left_conditions)):
                        left_condition = left_conditions[index]
                        right_condition = right_conditions[index]
                        comparator = comparators[index]
                        if common_building_blocks.judge_join_condition(foreign_record=record, key_record=record_to_join,
                                                                       left_condition=left_condition,
                                                                       right_condition=right_condition,
                                                                       comparator=comparator,
                                                                       table_names=self.__query['from']):
                            if index == len(left_conditions) - 1:
                                # if the WHERE condition applies as well, we execute the join
                                record = record[selected_columns[query['from'][1]]]
                                concatenated_record = dataframe_table.DataFrameTable(pd.concat([record, record_to_join], axis=0)).transpose()
                                concatenated_record.columns = query['select']
                                result_table = result_table.append(concatenated_record, ignore_index=True)
                                # registering the memory access pattern and the cost of writing
                                common_building_blocks.log_metrics(result_table.shape[0] - 1,
                                                                   False)
                            else:
                                # if the WHERE condition does not apply, we make a dummy write instead of joining the records
                                result_table = dataframe_table.DataFrameTable(pd.concat([result_table, pd.DataFrame([[np.nan] * result_table.shape[1]], columns=query['select'])], ignore_index=True))
                                # registering the memory access pattern and the cost of writing
                                common_building_blocks.log_metrics(result_table.shape[0] - 1,False)
                                break
                        # added else part now
                        else:
                            # if the WHERE condition does not apply, we make a dummy write instead of joining the records
                            result_table = dataframe_table.DataFrameTable(pd.concat([result_table, pd.DataFrame(
                                [[np.nan] * result_table.shape[1]], columns=query['select'])], ignore_index=True))
                            # registering the memory access pattern and the cost of writing
                            common_building_blocks.log_metrics(result_table.shape[0] - 1, False)
                            break
                else:
                    # if there is no match, we write a dummy record to the result table
                    result_table = dataframe_table.DataFrameTable(pd.concat([result_table, pd.DataFrame([[np.nan] * result_table.shape[1]], columns=query['select'])], ignore_index=True))
                    common_building_blocks.log_metrics(result_table.shape[0] - 1, False)  # registering the memory access pattern and the cost of writing
        print('HASH JOIN')
        print(result_table)
        end = time.time()
        cost_tracker.ObliviousTracker.register_execution_time(end - start)
        cost_tracker.ObliviousTracker.register_result_table_size(result_table.shape[0])

    def sort_merge_join(self, query):
        query = parser.QueryParser().parse(query)

        # getting the two database tables from the database
        no_tables = len(query['from'])
        if no_tables != 2:
            raise ValueError('Only 1 table allowed at filtering queries!')  # 2 tables are needed for the join query
        tables = []
        for i in range(no_tables):
            tables.append(self.__database.get_table(query['from'][i]))

        # columns to join on
        join_columns = query['join_fields']

        # WHERE condition parameters
        conditions = query['where']
        left_conditions, right_conditions, comparators = common_building_blocks.process_where_condition(conditions)

        # starting the timer that measures the execution time
        start = time.time()

        # STAGE 1: union
        unioned_table = dataframe_table.DataFrameTable(pd.concat(tables, ignore_index=True))

        unioned_table = self.__obliDB_blocks.bitonic_sort(unioned_table, join_columns[0], 0)

        #unioned_table = self.__opaque_blocks.column_sort(unioned_table, join_columns[0])

        # join them together to the unioned table
        # unioned_table = table.Table(None, pd.concat([Tp_sorted.get_table(), Tf_sorted.get_table()], ignore_index=True))

        # STAGE 2
        # partitions scanning
        partitions = self.__opaque_blocks.split_data_to_partitions(unioned_table)
        boundary_records = self.__obliDB_blocks.produce_boundary_records(partitions, join_columns[0])

        # STAGE 3
        # boundary procssing step
        processed_boundaries = common_building_blocks.boundary_record_processing(boundary_records)

        # STAGE 4
        # execute joins
        result = self.__obliDB_blocks.scan_join(partitions, processed_boundaries, join_columns[0], left_conditions, right_conditions, comparators, query=query)

        # STAGE
        # filtering out non values, filtering the table based on join attributes: we do not need this step, as
        # we are at oblivious padding mode
        #result = self.__opaque_blocks.column_sort(result, join_columns[0])
        #result = self.__obliDB_blocks.bitonic_sort(result, join_columns[0], 0, padding=True)

        # registering the cost elapsed during the join execution
        end = time.time()
        cost_tracker.ObliviousTracker.register_execution_time(end - start)
        cost_tracker.ObliviousTracker.register_result_table_size(result.shape[0])
        print(result)

        # Tracking the memory pattern and the execution cost of accessing the given row

    # I have implemented the double hash function based on the following tutorial:
    # https://www.geeksforgeeks.org/double-hashing/
    def __double_hash_function(self, index, result_table_size):
        hashed_index_1 = index % result_table_size
        # the prime value should be smaller than the table size
        prime = floor(result_table_size / 2)
        hash_index_2 = prime - (index % prime)

        return hashed_index_1 % result_table_size, (hashed_index_1 + hash_index_2) % result_table_size
