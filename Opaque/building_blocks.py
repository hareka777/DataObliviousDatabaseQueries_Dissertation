import pandas as pd
import numpy as np
import threading
import time
import math
from database import dataframe_table
from database import numpy_table
from MetricsTracker import cost_tracker
from CommonBuildingBlocks import common_building_blocks
from math import floor, ceil, log

class BuildingBlocks():

    # private variables
    __no_partitions = None
    __partitions = []
    __row_count_in_partition = None
    __column_list = None
    __oblivious_memory_size = None

    def __init__(self, no_partitions = 3, oblivious_memory_size = 0):
        self.__no_partitions = no_partitions
        self.__oblivious_memory_size = oblivious_memory_size

    # public operations
    def column_sort(self, table, sorting_key, padding=False):
        if sorting_key not in table.columns:
            raise ValueError('The key is not in the table!')
        else:
            # computing the number of machines
            size_of_table = table.memory_usage(deep=True).sum() * 10 ** -6
            self.__no_partitions = max(math.floor(size_of_table / self.__oblivious_memory_size), 1)
            print('Number of partitions:', self.__no_partitions)

            self.__reset_variables()
            partitions_list = self.split_data_to_partitions(data=table, no_partitions=self.__no_partitions)

            # making deep copy about the partitions
            for partition in partitions_list:
                self.__partitions.append(dataframe_table.DataFrameTable(partition.copy(deep=True)))

            # checking for partition size condition
            partition_length = self.__row_count_in_partition
            number_of_partitions = self.__no_partitions
            if (partition_length >= 2 * pow((number_of_partitions - 1),2)):
                print('success')
            else:
                print('Invalid partition sizes, bitonic sort over the blocks will be applied')
                print('Partition length = ',partition_length)
                print('Number of partitions = ', number_of_partitions)
                '''self.__partitions = []
                i = 0
                while i < len(partitions_list):
                    self.__partitions.append(dataframe_table.DataFrameTable(partitions_list[i].copy(deep=True)))
                    i += 2

                self.__sort_columns(sorting_key)

                sorted_partitions_up = []
                for partition in self.__partitions:
                    sorted_partitions_up.append(dataframe_table.DataFrameTable(partition.copy(deep=True)))

                self.__partitions = []
                i = 1
                while i < len(partitions_list):
                    self.__partitions.append(dataframe_table.DataFrameTable(partitions_list[i].copy(deep=True)))
                    i += 2
                self.__sort_columns(sorting_key, 1)

                sorted_partitions_down = []
                for partition in self.__partitions:
                    sorted_partitions_down.append(dataframe_table.DataFrameTable(partition.copy(deep=True)))

                self.__partitions = []
                for i in range(len(partitions_list)):
                    if i % 2 == 0:
                        self.__partitions.append(dataframe_table.DataFrameTable(sorted_partitions_up[int(i/2)].copy(deep=True)))
                    else:
                        self.__partitions.append(dataframe_table.DataFrameTable(sorted_partitions_down[int((i - 1)/2)].copy(deep=True)))
'''
                result = self.bitonic_sort_over_blocks(sorting_key)
                return dataframe_table.DataFrameTable(result)


            # step 1: sorting partitions
            self.__sort_columns(sorting_key)



            #step 2: shuffle 1 - transpose

            # concatenating the matrix to a numpy array
            # turning the tracking_enabled flag off
            matrix = self.__concat_partitions_into_matrix()

            matrix = matrix.transpose()
            self.__log_transpose_operation_costs(matrix.shape[0], matrix.shape[1])

            matrix = matrix.reshape(self.__row_count_in_partition, self.__no_partitions)

            # converting back to dataframe partitions
            self.__separate_matrix_to_partitions(matrix)

            # step 3: sorting partitions
            self.__sort_columns(sorting_key)


            # step 4: inverse transpose

            matrix = self.__concat_partitions_into_matrix()

            matrix = matrix.reshape(self.__no_partitions, self.__row_count_in_partition)

            matrix = matrix.transpose()
            self.__log_transpose_operation_costs(matrix.shape[0], matrix.shape[1])

            # converting back to dataframe partitions
            self.__separate_matrix_to_partitions(matrix)

            # step 5: sorting partitions
            self.__sort_columns(sorting_key)

            # step 6: shifting step
            partition_length = self.__row_count_in_partition
            rows_to_shift_forward = int(math.ceil(partition_length / 2))
            rows_to_shift_backward = int(math.floor(partition_length / 2))

            #self.__shift_right()
            # shift rows
            for i in range(len(self.__partitions)):
                current_partition = self.__partitions[i]

                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
                self.__partitions[i] = pd.concat((current_partition[rows_to_shift_forward:partition_length],
                                                  current_partition[0:rows_to_shift_forward]))
                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

            # shift columns between partitions
            partition_slice_to_shift = self.__partitions[self.__no_partitions - 1][0:rows_to_shift_backward].copy(
                deep=True)
            for i in range(len(self.__partitions)):
                partition_slice_to_insert = partition_slice_to_shift.copy(deep=True)
                partition_slice_to_shift = self.__partitions[i][0:rows_to_shift_backward].copy(deep=True)

                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
                self.__partitions[i][0:rows_to_shift_backward] = partition_slice_to_insert
                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

            first_partition = self.__partitions[0]
            self.__partitions = self.__partitions[1:self.__no_partitions]

            # step 7: sorting partitions
            self.__sort_columns(sorting_key)
            self.__partitions.insert(0, first_partition)

            '''splitted_partitions = self.__partitions
            self.__partitions[0] = first_partition
            self.__partitions[1:self.__no_partitions] = splitted_partitions'''

            # step 8: inverse shifting
            #self.__shift_left()

            # shift columns between partitions
            partition_slice_to_shift = self.__partitions[0][0:rows_to_shift_forward].copy(deep=True)
            for i in reversed(range(len(self.__partitions))):
                partition_slice_to_insert = partition_slice_to_shift.copy(deep=True)
                partition_slice_to_shift = self.__partitions[i][0:rows_to_shift_forward].copy(deep=True)

                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
                self.__partitions[i][0:rows_to_shift_forward] = partition_slice_to_insert
                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

            # change top and bottom sub-partitions
            for i in range(0, len(self.__partitions)):
                current_partition = self.__partitions[i]

                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
                self.__partitions[i] = pd.concat((current_partition[rows_to_shift_backward:partition_length],
                                                  current_partition[0:rows_to_shift_backward]))
                cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)



            # concatenating the sorted partitions
            result = self.__concatenate_partitions()

            # dropping null rows
            # when in oblivious padding mode, we keep these rows
            if padding == False:
                result = result.dropna(how='all')

            return dataframe_table.DataFrameTable(result)

    def intra_machine_sorting(self, table, sorting_key, sort_dir):
        if sorting_key not in table.columns:
            raise ValueError('The key is not in the table!')
        else:
            asceding = True
            if sort_dir is not None:
                if sort_dir == 1:
                    asceding = False
            # assume the whole table fits to the oblivious memory one time
            if 'DisplayName' in table.columns:
                # including 'DisplayName' as a sorting key, because it breaks ties when the sorting keys are equal
                # it sorts the key table elements before the foreign key table elements, because ID = nan there
                table.sort_values(by=[sorting_key, 'DisplayName'], kind='quicksort', inplace=True, ascending=asceding)
            else:
                table.sort_values(by=[sorting_key], kind='quicksort', inplace=True, asceding=asceding)

    def split_data_to_partitions(self, data, no_partitions=3):

        dummy_data = data
        self.__column_list = data.columns

        # adding dummy rows if needed
        modulo = (data.shape[0] % no_partitions)

        if modulo != 0:
            for i in range(self.__no_partitions - modulo):
                dummy_data = dataframe_table.DataFrameTable(pd.concat([dummy_data, pd.DataFrame([[np.nan] * dummy_data.shape[1]], columns=self.__column_list)], ignore_index=True))


        # splitting data to partitions
        partitions = []

        for i in range(no_partitions):
            partition_start_index = i*int(dummy_data.shape[0]/(no_partitions))
            partition_end_index = (i+1)*int(dummy_data.shape[0]/(no_partitions)) - 1
            partitions.append(dataframe_table.DataFrameTable(dummy_data.loc[partition_start_index:partition_end_index]))

        self.__row_count_in_partition = partitions[0].shape[0]

        return partitions

    def bitonic_sort_over_blocks(self, sorting_key):

        concatenated_partitions = pd.DataFrame()
        for partition in self.__partitions:
            concatenated_partitions = pd.concat([concatenated_partitions, partition], ignore_index=True)



        # step 2: executing bitonic sort over the blocks
        result = self.bitonic_sort(concatenated_partitions, sorting_key, 0)
        return result

    # private operations
    def __sort_columns(self, sort_key, sort_dir = None):

        try:

            thread_pool = []
            for partition in self.__partitions:
                thread = PartitionSortThread(partition, sort_key, sort_dir)
                thread_pool.append(thread)
                thread.start()

            for thread in thread_pool:
                thread.join()

        except:
            print("Error: unable to start thread")

    def __concat_partitions_into_matrix(self):
        matrix = np.empty([self.__row_count_in_partition, self.__no_partitions] , dtype=pd.Series)

        for column_idx in range(self.__no_partitions):
            partition = self.__partitions[column_idx]
            row_idx = 0
            for row in partition.itertuples():
                row_tuple = tuple(row)
                matrix[row_idx, column_idx] = row_tuple[1:len(row_tuple)]
                row_idx += 1

        return matrix.view(numpy_table.NumpyTable)

    '''def __concat_partitions_into_matrix(self):
        matrix = np.empty([self.__row_count_in_partition, self.__no_partitions] , dtype=pd.Series)

        for column_idx in range(self.__no_partitions):
            partition = self.__partitions[column_idx]
            for row_idx in range(partition.shape[0]):
                row = partition.iloc[row_idx]
                matrix[row_idx, column_idx] = row[1:len(row)]

        return matrix.view(numpy_table.NumpyTable)'''

    def __separate_matrix_to_partitions(self, matrix):

        for column_idx in range(self.__no_partitions):
            partition = matrix[:,column_idx]
            partition = dataframe_table.DataFrameTable(pd.DataFrame.from_records(partition, columns=self.__column_list))
            self.__partitions[column_idx] = partition

    def __concatenate_partitions(self):
        concatenated_result = self.__partitions[0]
        for i in range(1, self.__no_partitions):
            concatenated_result = dataframe_table.DataFrameTable(pd.concat((concatenated_result, self.__partitions[i]), ignore_index=True))
        return  concatenated_result

    def __reset_variables(self):
        self.__partitions = []
        self.__row_count_in_partition = None
        self.__column_list = None

    # bitonic sort code for already sorted partitions
    def bitonic_sort(self, data, key, sorting_direction, padding=False):

        number_of_elements = self.__reducing_array(data)
        self.__count = 0
        self.__bitonic_sort(data, key, 0, number_of_elements, sorting_direction)

        self.__sort_leftover_elements(data, number_of_elements, key, sorting_direction)

        # if we are in oblivious padding, mode, to provide the same output table size, we can't drop the dummy records
        if padding == False:
            data = data.dropna(how='all')

        return data

    def __bitonic_sort(self, data, key, bottom_index, count, sorting_direction):
        # step 1: creating bitonic sequence

        if count > 1:
            # splitting the data into two partitions
            size_of_data = count
            partition_size = int(size_of_data / 2)
            partition_1_first_element = bottom_index
            partition_2_first_element = bottom_index + partition_size


            # executing bitonic sort on the two partitions
            # sorting partition 1 in ascending order while partition 2 in descending order to be able
            # to make a bitonic sequence
            self.__bitonic_sort(data, key, partition_1_first_element, partition_size, 0)
            self.__bitonic_sort(data, key, partition_2_first_element, partition_size, 1)

            # merge the sequences into an ascending/descending ordered sequence
            self.__bitonic_merge(data, key, partition_1_first_element, size_of_data, sorting_direction)

    # recursively sort and merge the elements into the correct order
    def __bitonic_merge(self, data, key, bottom_index, count, sorting_direction):
        if count > self.__partitions[0].shape[0] * 2:
            size_of_data = count
            partition_size = int(size_of_data / 2)

            '''
            data_memory = float(data[bottom_index:bottom_index + size_of_data].memory_usage(deep=True).sum()) * 10 ** -6
            available_data_memory = self.__enclave_size / 1.5
            if data_memory <= available_data_memory:
                self.__enclave_size = True'''


            # compare and swap all ith and (i + partition_size)th elements
            for i in range(bottom_index, bottom_index + partition_size):
                self.__compare_and_swap_elements(data, i, i + partition_size, key, sorting_direction)

            # call bitonic merge at the splitted datasets to compare and swap their ith and (i + partition_size)th elements
            self.__bitonic_merge(data, key, bottom_index, partition_size, sorting_direction)
            self.__bitonic_merge(data, key, bottom_index + partition_size, partition_size, sorting_direction)

        else:
            ascending = True
            if sorting_direction == 1:
                ascending = False
            values_to_sort = data[bottom_index:bottom_index + count].copy(deep=True)
            values_to_sort.sort_values(by=[key, 'DisplayName'], kind='quicksort', inplace=True, ascending=ascending)
            data[bottom_index:bottom_index + count] = values_to_sort


    def __compare_and_swap_elements(self, data, element1_index, element2_index, key, direction):
        self.__count += 4
        # executing inplace comparing and swapping
        # reading the writing the elements here gives the oblivious memeory accesses

        #print('{} : {}'.format(element1_index, element2_index))
        # reading in the elements
        cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
        element1 = data.loc[element1_index]
        cost_tracker.ObliviousTracker.register_cost(1, True, element1_index)
        cost_tracker.ObliviousTracker.register_memory_access(element1_index, True)

        element2 = data.loc[element2_index]
        cost_tracker.ObliviousTracker.register_cost(1, True, element2_index)
        cost_tracker.ObliviousTracker.register_memory_access(element2_index, True)
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
            elif int(element1[key]) > int(element2[key]):
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
            elif int(element1[key]) > int(element2[key]):
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

        # Scans the array to insert the element to the list
    def __sort_leftover_elements(self, data, boundary, key, direction):
        #print(data.loc[boundary])
        for index in range(boundary, data.shape[0]):
            for i in range(boundary):
                self.__compare_and_swap_elements(data, i, index, key, direction)
            boundary += 1

    def __log_writing_elements(self, element1_index, element2_index):
        cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
        cost_tracker.ObliviousTracker.register_cost(1, False, element1_index)
        cost_tracker.ObliviousTracker.register_memory_access(element1_index, False)
        cost_tracker.ObliviousTracker.register_cost(1, False, element2_index)
        cost_tracker.ObliviousTracker.register_memory_access(element2_index, False)
        cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

    '''def __transpose(self, table):
        transposed_table = np.empty((table.shape[1], table.shape[0]), dtype=table.dtype).view(numpy_table.NumpyTable)

        for row in range(table.shape[0]):
            for column in range(table.shape[1]):
                transposed_table[column, row] = table[row, column]
        return transposed_table
'''
    def __log_transpose_operation_costs(self, number_of_rows, number_of_columns):
        for row in range(number_of_rows):
            for column in range(number_of_columns):
                read_element = row * number_of_columns + column
                written_element = column * number_of_rows + row
                common_building_blocks.log_metrics(accessed_index=read_element, read=True)
                common_building_blocks.log_metrics(accessed_index=written_element, read=False)


    '''def __shift_right(self):
        partition_length = self.__row_count_in_partition
        rows_to_shift_forward = int(math.floor(partition_length / 2))

        # removing the last and the first partitions
        first_new_partition = None
        last_new_partition = None

        # shift partitions to right, floor(r/2) shift
        shift_step = int(math.floor(partition_length / 2))
        new_partitions = {}
        for i in range(len(self.__partitions)):
            current_partition = self.__partitions[i]
            current_partition_first_half = current_partition[0:rows_to_shift_forward]
            current_partition_second_half = current_partition[rows_to_shift_forward:partition_length]
            first_half_partition_index = i
            second_half_partition_index = i + len(self.__partitions)

            # execute shifting
            index_to_shift = (first_half_partition_index + shift_step) % (2 * self.__no_partitions)
            if index_to_shift < self.__no_partitions:
                index_to_shift = (index_to_shift + 1) % self.__no_partitions

            new_partitions[index_to_shift] = current_partition_first_half
            index_to_shift = (second_half_partition_index + shift_step) % (2 * self.__no_partitions)
            if index_to_shift < self.__no_partitions:
                index_to_shift = (index_to_shift + 1) % self.__no_partitions
            new_partitions[index_to_shift] = current_partition_second_half

        # updating __self.partitions
        for partition_index in range(self.__no_partitions):
            self.__partitions[partition_index] = pd.concat(
                [new_partitions[partition_index], new_partitions[partition_index + self.__no_partitions]],
                ignore_index=True)

        print('OK')'''

    '''def __shift_left(self):
        partition_length = self.__row_count_in_partition
        rows_to_shift_forward = int(math.floor(partition_length / 2))

        # shift partitions to right, floor(r/2) shift
        shift_step = int(math.floor(partition_length / 2))
        new_partitions = {}
        for i in range(len(self.__partitions)):
            current_partition = self.__partitions[i]
            current_partition_first_half = current_partition[0:rows_to_shift_forward]
            current_partition_second_half = current_partition[rows_to_shift_forward:partition_length]
            first_half_partition_index = i
            second_half_partition_index = i + len(self.__partitions)

            # execute shifting
            if first_half_partition_index < self.__no_partitions:
                index_to_shift = (first_half_partition_index - 1) % self.__no_partitions
            else:
                index_to_shift = first_half_partition_index
            index_to_shift = (index_to_shift - shift_step) % (2 * self.__no_partitions)
            new_partitions[index_to_shift] = current_partition_first_half

            if second_half_partition_index < self.__no_partitions:
                index_to_shift = (second_half_partition_index - 1) % self.__no_partitions
            else:
                index_to_shift = second_half_partition_index
            index_to_shift = (index_to_shift - shift_step) % (2 * self.__no_partitions)
            new_partitions[index_to_shift] = current_partition_second_half

        # updating __self.partitions
        for partition_index in range(self.__no_partitions):
            self.__partitions[partition_index] = pd.concat(
                [new_partitions[partition_index], new_partitions[partition_index + self.__no_partitions]],
                ignore_index=True)

        print('OK')'''

class PartitionSortThread(threading.Thread):
   def __init__(self, partition, sorting_key, sort_dir = None):
      threading.Thread.__init__(self)
      self.partition = partition
      self.sorting_key = sorting_key
      self.sorting_direction = sort_dir

   def run(self):
      sort_column_thread_function(self.partition, self.sorting_key, self.sorting_direction)

def sort_column_thread_function(partition, sort_key, sort_dir):
    return BuildingBlocks().intra_machine_sorting(partition, sort_key, sort_dir)