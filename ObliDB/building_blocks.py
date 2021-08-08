import pandas as pd
from database import dataframe_table
from math import ceil, log, floor
import numpy as np
from MetricsTracker import cost_tracker
from CommonBuildingBlocks import common_building_blocks

# I have implemented this section using the following tutorials, pseudocodes and examples:
# https://www.youtube.com/watch?v=uEfieI0MumY
# https://www.youtube.com/watch?v=GEQ8y26blEY
# https://www.inf.hs-flensburg.de/lang/algorithmen/sortieren/bitonic/bitonicen.htm

class BuildingBlocks():
    __enclave_usage = None
    __enclave_size = 0
    __count = 0

    def __init__(self, enclave_size = 0):
        self.__enclave_usage = False
        self.__enclave_size = enclave_size

    def bitonic_sort(self, data, key, sorting_direction, padding=False):
        #data = self.__extend_with_dummies(data)
        number_of_elements = self.__reducing_array(data)
        self.__count = 0

        self.__bitonic_sort(data, key, 0, number_of_elements, sorting_direction)
        #print('All operations: ', self.__count)
        self.__sort_leftover_elements(data, number_of_elements, key, sorting_direction)

        # if we are in oblivous padding, mode, to provide the same output table size, we can't drop the dummy records
        if padding == False:
            data = data.dropna(how='all')
        return data

    def __bitonic_sort(self, data, key, bottom_index, count, sorting_direction):
        # step 1: creating bitonic sequence
        self.__enclave_usage = False
        if count > 1:
            # splitting the data into two partitions
            size_of_data = count
            partition_size = int(size_of_data / 2)
            partition_1_first_element = bottom_index
            partition_2_first_element = bottom_index + partition_size



            #print(data[bottom_index:partition_2_first_element].shape[0])
            #print(data[partition_2_first_element:partition_2_first_element + partition_size].shape[0])

            # executing bitonic sort on the two partitions
            # sorting partition 1 in ascending order while partition 2 in descending order to be able
            # to make a bitonic sequence
            self.__bitonic_sort(data, key, partition_1_first_element, partition_size, 0)
            self.__bitonic_sort(data, key, partition_2_first_element, partition_size, 1)

            # merge the sequences into an ascending/descending ordered sequence
            self.__bitonic_merge(data, key, partition_1_first_element, size_of_data, sorting_direction)

    # recursively sort and merge the elements into the correct order
    def __bitonic_merge(self, data, key, bottom_index, count, sorting_direction):
        data_memory = float(data[bottom_index:bottom_index +count].memory_usage(deep=True).sum()) * 10 ** -6

        #print(data_memory)
        self.__enclave_usage = False
        available_data_memory = self.__enclave_size
        if data_memory <= available_data_memory:
            self.__enclave_usage = True

        if count > 1:
            size_of_data = count
            partition_size = int(size_of_data / 2)


            # compare and swap all ith and (i + partition_size)th elements
            for i in range(bottom_index, bottom_index + partition_size):
                self.__compare_and_swap_elements(data, i, i + partition_size, key, sorting_direction)

            # call bitonic merge at the splitted datasets to compare and swap their ith and (i + partition_size)th elements
            self.__bitonic_merge(data, key, bottom_index, partition_size, sorting_direction)
            self.__bitonic_merge(data, key, bottom_index + partition_size, partition_size, sorting_direction)


    def __compare_and_swap_elements(self, data, element1_index, element2_index, key, direction):
        self.__count += 4
        # executing inplace comparing and swapping
        # reading the writing the elements here gives the oblivious memeory accesses

        #print('{} : {}'.format(element1_index, element2_index))
        # reading in the elements
        if self.__enclave_usage == False:
            cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
        element1 = data.loc[element1_index]
        cost_tracker.ObliviousTracker.register_cost(1, True, element1_index)
        cost_tracker.ObliviousTracker.register_memory_access(element1_index, True)

        element2 = data.loc[element2_index]
        cost_tracker.ObliviousTracker.register_cost(1, True, element2_index)
        cost_tracker.ObliviousTracker.register_memory_access(element2_index, True)
        if self.__enclave_usage == False:
            cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

        # comparing the elements based on the direction condition:
        # direction = 0, ascending order
        # direction = 1, descending order
        if direction == 0:
            # we always sort the np.nan elements back independently from the direction
            if pd.isnull(element1[key]) or pd.isnull(element2[key]):
                if pd.isnull(element1[key]):
                    data.at[element1_index] = element2
                    data.at[element2_index] = element1
                    self.__log_writing_elements(element1_index, element2_index)

                else:
                    data.at[element1_index] = element1
                    data.at[element2_index] = element2
                    self.__log_writing_elements(element1_index, element2_index)
            elif element1[key] > element2[key]:
                data.at[element1_index] = element2
                data.at[element2_index] = element1
                self.__log_writing_elements(element1_index, element2_index)
            else:
                data.at[element1_index] = element1
                data.at[element2_index] = element2
                self.__log_writing_elements(element1_index, element2_index)

        else:
            if pd.isnull(element1[key]) or pd.isnull(element2[key]):
                if pd.isnull(element1[key]):
                    data.at[element1_index] = element1
                    data.at[element2_index] = element2
                    self.__log_writing_elements(element1_index, element2_index)
                else:
                    data.at[element1_index] = element2
                    data.at[element2_index] = element1
                    self.__log_writing_elements(element1_index, element2_index)
            elif element1[key] > element2[key]:
                data.at[element1_index] = element1
                data.at[element2_index] = element2
                self.__log_writing_elements(element1_index, element2_index)
            else:
                data.at[element1_index] = element2
                data.at[element2_index] = element1
                self.__log_writing_elements(element1_index, element2_index)

    def __extend_with_dummies(self, data):
        data_size = data.shape[0]

        # finding the next power of 2 number:
        next_pow = ceil(log(data_size, 2))
        new_size = pow(2, next_pow)
        rows_to_add = new_size - data_size
        for i in range(rows_to_add):
            data = dataframe_table.DataFrameTable(pd.concat([data, pd.DataFrame([[np.nan] * data.shape[1]], columns=data.columns)], ignore_index=True))
        return data

    # Calculates the number that is the form of 2^x, where x is a positive integer
    # Returns the number of elements the bitonic sort should include
    def __reducing_array(self, data):
        data_size = data.shape[0]
        last_pow = floor(log(data_size, 2))
        return 2 ** last_pow

    def __log_writing_elements(self, element1_index, element2_index):
        if self.__enclave_usage == False:
            cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
            cost_tracker.ObliviousTracker.register_cost(1, False, element1_index)
            cost_tracker.ObliviousTracker.register_memory_access(element1_index, False)
            cost_tracker.ObliviousTracker.register_cost(1, False, element2_index)
            cost_tracker.ObliviousTracker.register_memory_access(element2_index, False)
            cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

    # Scans the array to insert the element to the list
    def __sort_leftover_elements(self, data, boundary, key, direction):
        for i in range(data.shape[0] - boundary):
            for i in range(boundary):
                self.__enclave_usage = False
                self.__compare_and_swap_elements(data, i, boundary, key, direction)
            boundary += 1

    def scan_join(self, partitions, boundaries, join_attribute, left_conditions, right_conditions, comparators, query):
        # iterating over partitions
        result_table = dataframe_table.DataFrameTable(pd.DataFrame(columns=partitions[0].columns))
        for index in range(len(boundaries)):
            partition = partitions[index]
            boundary = boundaries[index]
            # scanning the partitions
            last_key_record = boundary

            for row_idx in range(partition.shape[0]):

                row = partition.iloc[row_idx]
                # Logging the memory access and cost
                common_building_blocks.log_metrics(row_idx, read=True)

                # flag indicating if we have a match
                match_flag = False

                # if this is a key element, return dummy record
                # DisplayName is a unique column in the key table, so we can decide if it's a primary key table record
                if pd.isnull(getattr(row, 'DisplayName')) == False:
                    result_table = result_table.append(
                        dataframe_table.DataFrameTable(
                            pd.DataFrame([[np.nan] * partition.shape[1]], columns=partition.columns)),
                        ignore_index=True)
                    last_key_record = row

                else:
                    # we decide if there is a key element if the field field(that cannot be none) in the key table is not nan
                    # if this is a primary key record, we make a dummy write
                    if left_conditions is not None:
                        for index in range(len(left_conditions)):
                            left_condition = left_conditions[index]
                            right_condition = right_conditions[index]
                            comparator = comparators[index]
                            if common_building_blocks.judge_join_condition(foreign_record=row,
                                                                           key_record=last_key_record,
                                                                           left_condition=left_condition,
                                                                           right_condition=right_condition,
                                                                           comparator=comparator,
                                                                           table_names=query['from']):
                                if index == len(left_conditions) - 1:
                                    if last_key_record.empty == False:
                                        if getattr(row, join_attribute) == getattr(last_key_record, join_attribute):
                                            match_flag = True
                                            print('RealMatch')
                            else:
                                break
                    else:
                        match_flag = True

                    # if the where condition applies, we add the joined row to the result table
                    # otherwise, we make a dummy write
                    if match_flag == True:
                        # join with the key record, we are ignoring the index(first element of row)
                        joined_record = dataframe_table.DataFrameTable(
                            pd.DataFrame([[np.nan] * partition.shape[1]], columns=partition.columns))
                        '''row = row.to_frame().transpose()
                        row = row.replace(np.nan, None)
                        last_key_record = last_key_record.to_frame().transpose()
                        last_key_record = last_key_record.replace(np.nan, None)
                        joined_record = pd.merge(row, last_key_record, on=join_attribute, suffixes=None)'''
                        # creating the record to join
                        for column in partition.columns:
                            if getattr(row, column) is not np.nan:
                                joined_record[column] = getattr(row, column)
                            else:
                                joined_record[column] = getattr(last_key_record, column)
                        result_table = result_table.append(joined_record, ignore_index=True)
                    else:
                        # adding a dummy record to the result table
                        # row = np.array(row)[1:].reshape(1, partition.shape[1])
                        '''if pd.isnull(row['DisplayName']) == False:
                            last_key_record = row'''
                        result_table = result_table.append(dataframe_table.DataFrameTable(
                            pd.DataFrame([[np.nan] * partition.shape[1]], columns=partition.columns)),
                                                           ignore_index=True)
                common_building_blocks.log_metrics(result_table.shape[0] - 1, read=False)

        return result_table

    def produce_boundary_records(self, partitions, key):
        boundary_records = []
        for partition in partitions:
            partition_boundary = None
            # we decide if there is a key element if the field field(that cannot be none) in the key table is not nan
            for row_idx in range(partition.shape[0]):
                row = partition.iloc[row_idx]
                # Registering the memory access patterns and costs
                common_building_blocks.log_metrics(row_idx, read=True)

                if pd.isnull(getattr(row, 'DisplayName')) == False:
                    partition_boundary = row
            boundary_records.append(partition_boundary)
            # registering reading operation
            common_building_blocks.log_metrics(len(boundary_records) - 1, read=False)
        return boundary_records