from MetricsTracker import cost_tracker
import pandas as pd
from database import dataframe_table
from CommonBuildingBlocks import common_building_blocks
import numpy as np
import threading

def process_where_condition(where_conditions):

    left_conditions = []
    right_conditions = []
    comparators = []


    for condition in where_conditions:
        if '==' in condition:
            where_condition = condition.split('==')
            left_conditions.append(where_condition[0])
            right_conditions.append(where_condition[1])
            comparators.append('==')
        elif '>' in condition:
            where_condition = condition.split('>')
            left_conditions.append(where_condition[0])
            right_conditions.append(where_condition[1])
            comparators.append('>')
        elif '<' in condition:
            where_condition = condition.split('<')
            left_conditions.append(where_condition[0])
            right_conditions.append(where_condition[1])
            comparators.append('<')
        elif '!=' in condition:
            where_condition = condition.split('!=')
            left_conditions.append(where_condition[0])
            right_conditions.append(where_condition[1])
            comparators.append('!=')

    return left_conditions, right_conditions, comparators


def judge_condition(row, left_condition, right_condition, comparator):
    '''
    Checks if the WHERE condition of the query is true or false
    :param row: the database row where we would like to check the thruthness of the 'where' condition,
    e.g. at 'Price < 5000' query, this is the 'Price'
    :param left_condition: the column of the where condition (left side)
    :param right_condition: the value we need to compare our row (right side of the condition),
    e.g. at 'Price < 5000' query, this is the '5000'
    :param comparator: the condition type, like <,>,==,!=
    :return: Returns a bool depends on the truthness of the query's where condition(s)
    '''

    left_condition = getattr(row, left_condition)

    # there is a bug in pandas merge, concat etc. methods: it does convert the original column types to another one
    # this bug causes a bug in our case, when it converts integer values to float, to solve this problem,
    # I decided to add this 'hack' to the code:
    #if type(left_condition) == float:
    #    left_condition = int(left_condition)

    if comparator == '==':
        if str(left_condition) == right_condition:
            return True
        else:
            return False

    elif comparator == '>':
        if str(left_condition) > right_condition:
            return True
        else:
            return False

    elif comparator == '<':
        if str(left_condition) < right_condition:
            return True
        else:
            return False

    elif comparator == '!=':
        if str(left_condition) != right_condition:
            return True
        else:
            return False

def judge_join_condition(foreign_record, key_record, left_condition, right_condition, comparator, table_names):
    '''
    Checks if the WHERE condition of the query is true or false in case of join queries where we join multiple tables
    and therefore, we need to distinguish between the joined tables
    :param foreign_record: record of the foreign key table that we would like to join
    :param key_record: record of the primary key table that we would like to join
    :param left_condition: the column of the where condition (left side)
    e.g. at 'Cars.Price < 5000' query, this is the 'Cars.Price'
    :param right_condition: the value we need to compare our row (right side of the condition),
    e.g. at 'Cars.Price < 5000' query, this is the '5000'
    :param comparator: the condition type, like <,>,==,!=
    :param table_names: list of the name of tables to be joined, the query's FROM part
    :return:
    '''

    # deciding which is the table where we need to check the WHERE condition
    left_condition_parts = left_condition.split('.')
    table_name =  left_condition_parts[0]
    condition_field = left_condition_parts[1]

    if table_name == table_names[0]:
        record_to_check = key_record
    else:
        record_to_check = foreign_record

    truth = judge_condition(record_to_check, condition_field, right_condition, comparator)
    return truth

def log_metrics(accessed_index, read):
    '''
    Tracking the memory pattern and the execution cost of accessing the given row
    :param accessed_index: the location of the accessed memory element
    :param read: bool, indictaes if this is a writing or reading operation
    :return: None
    '''
    cost_tracker.ObliviousTracker.set_tracking_enabled_flag(True)
    cost_tracker.ObliviousTracker.register_cost(1, read, accessed_index)
    cost_tracker.ObliviousTracker.register_memory_access(accessed_index, read)
    cost_tracker.ObliviousTracker.set_tracking_enabled_flag(False)

def scan_join(partitions, boundaries, join_attribute, left_conditions, right_conditions, comparators, query):
    '''
    Executing the joins. The joins executed using the machines. The machines can work independently from each other,
    therefore, this step can be executed in parallel.
    :param partitions:
    :param boundaries:
    :param join_attribute:
    :param left_conditions:
    :param right_conditions:
    :param comparators:
    :param query:
    :return:
    '''
    # the table need to be returned
    result_tables = {}
    try:

        thread_pool = []
        for boundary_index in range(len(boundaries)):
            # setting up the parameters for the thread function
            partition = partitions[boundary_index]
            boundary = boundaries[boundary_index]
            # logging the boundary record read in operation
            #common_building_blocks.log_metrics(accessed_index=boundary_index, read=True)
            result_tables[boundary_index] = []
            # scanning the partitions
            thread = ScanJoinThread(partition, boundary, join_attribute, left_conditions, right_conditions, comparators, query, result_tables[boundary_index], boundary_index)
            thread_pool.append(thread)
            thread.start()

        for thread in thread_pool:
            thread.join()
    except:
        print("Error: unable to start thread")
    result_table_to_return = dataframe_table.DataFrameTable(pd.DataFrame(columns=partitions[0].columns))
    for result_table_index in range(len(result_tables)):
        result_table_to_return = pd.concat([result_table_to_return, result_tables[result_table_index][0]], ignore_index=True)

    # registering we wrote the result table
    for idx in range(result_table_to_return.shape[0]):
        common_building_blocks.log_metrics(accessed_index=idx, read=False)
    return result_table_to_return

def produce_boundary_records(partitions, key):
    boundaries = {}
    try:

        thread_pool = []
        partition_index = 0
        for partition in partitions:
            boundaries[partition_index] = []
            thread = PerPartitionScanThread(partition, boundaries[partition_index])
            thread_pool.append(thread)
            thread.start()
            partition_index += 1

        for thread in thread_pool:
            thread.join()
    except:
        print("Error: unable to start thread")

    boundaries_to_return = []
    for boundary_idx in range(len(boundaries)):
        boundaries_to_return.append(boundaries[boundary_idx][0])
    return boundaries_to_return

def boundary_record_processing(boundary_records):
    processed_boundaries = []
    processed_boundaries.append(None)
    for index in range(1, len(boundary_records)):
        # finding the last record that was not np.nan
        processed_record = np.nan
        record_found = False
        for idx in reversed(range(index)):
            if boundary_records[idx] is not None:
                if record_found != True:
                    processed_record = boundary_records[idx]
                    record_found = True
            # registering memory access
            common_building_blocks.log_metrics(accessed_index= idx, read=True)
        processed_boundaries.append(processed_record)
        # registering writing the output
        common_building_blocks.log_metrics(accessed_index=len(processed_boundaries) - 1, read=False)
    return processed_boundaries

class PerPartitionScanThread(threading.Thread):
   def __init__(self, partition, boundaries):
      threading.Thread.__init__(self)
      self.partition = partition
      self.boundaries = boundaries

   def run(self):
      boundary_record_producing_thread_function(self.partition, self.boundaries)

def boundary_record_producing_thread_function(partition, boundaries):
    partition_boundary = None
    # we decide if there is a key element if the field field(that cannot be none) in the key table is not nan
    for row_idx in range(partition.shape[0]):
        row = partition.iloc[row_idx]
        # We do not register the cost, because we already read it in!
        # common_building_blocks.log_metrics(row_idx, read=True)

        if pd.isnull(getattr(row, 'DisplayName')) == False:
            partition_boundary = row
    boundaries.append(partition_boundary)
    # registering memory access
    common_building_blocks.log_metrics(accessed_index=len(boundaries) - 1, read=False)

class ScanJoinThread(threading.Thread):
   def __init__(self, partition, boundary, join_attribute, left_conditions, right_conditions, comparators, query, result_table, boundary_index):
      threading.Thread.__init__(self)
      self.partition = partition
      self.boundary = boundary
      self.join_attribute = join_attribute
      self.left_conditions = left_conditions
      self.right_conditions = right_conditions
      self.comparators = comparators
      self.query = query
      self.result_table = result_table
      self.boundary_index = boundary_index

   def run(self):
      scan_join_thread_function(self.partition, self.boundary, self.join_attribute, self.left_conditions, self.right_conditions, self.comparators, self.query, self.result_table, self.boundary_index)

def scan_join_thread_function(partition, boundary, join_attribute, left_conditions, right_conditions, comparators, query, result_table, boundary_index):
    cost_tracker.ObliviousTracker.set_tracking_enabled_flag(flag=False)
    last_key_record = boundary
    # reading in boundary record
    common_building_blocks.log_metrics(accessed_index=boundary_index, read=True)
    results = dataframe_table.DataFrameTable(pd.DataFrame(columns=partition.columns))
    for row_idx in range(partition.shape[0]):
        #partition = partition.reset_index()

        row = partition.iloc[row_idx]
        # Logging the memory access and cost
        # common_building_blocks.log_metrics(row_idx, read=True)

        # if this is a key element, return dummy record
        # DisplayName is a unique column in the key table, so we can decide if it's a primary key table record
        if pd.isnull(getattr(row, 'DisplayName')) == False:
            results = results.append(pd.DataFrame([[np.nan] * partition.shape[1]], columns=partition.columns), ignore_index=True)
            last_key_record = row
            # registering writing operation after writing a dummy element
            #common_building_blocks.log_metrics(accessed_index=row_idx, read=False)
        else:

            # flag indicating if we have a match
            match_flag = False

            # we decide if there is a key element if the field field(that cannot be none) in the key table is not nan
            # if this is a primary key record, we make a dummy write
            if left_conditions is not None:
                for index in range(len(left_conditions)):
                    left_condition = left_conditions[index]
                    right_condition = right_conditions[index]
                    comparator = comparators[index]
                    if common_building_blocks.judge_join_condition(foreign_record=row, key_record=last_key_record,
                                                                   left_condition=left_condition,
                                                                   right_condition=right_condition,
                                                                   comparator=comparator,
                                                                   table_names=query['from']):
                        if index == len(left_conditions) - 1:
                            if last_key_record is not None:
                                if last_key_record.empty == False:
                                    if getattr(row, join_attribute) == getattr(last_key_record, join_attribute):
                                        match_flag = True
                                        print('RealMatch')
                    else:
                        break
            else:
                match_flag = False

            # if the where condition applies, we add the joined row to the result table
            # otherwise, we make a dummy write
            if match_flag == True:
                # join with the key record, we are ignoring the index(first element of row)
                joined_record = pd.DataFrame([[np.nan] * partition.shape[1]], columns=partition.columns)
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
                results = results.append(joined_record, ignore_index=True)
                # registering writing a record
                #common_building_blocks.log_metrics(accessed_index=row_idx, read=False)
            else:
                # adding a dummy record to the result table
                # row = np.array(row)[1:].reshape(1, partition.shape[1])
                '''if pd.isnull(row['DisplayName']) == False:
                    last_key_record = row'''
                results = results.append(pd.DataFrame([[np.nan] * partition.shape[1]], columns=partition.columns), ignore_index=True)
                # registering writing operation
                #common_building_blocks.log_metrics(accessed_index=row_idx, read=False)
        # we made a write operation anyway
        #common_building_blocks.log_metrics(accessed_index=row_idx, read=False)
    result_table.append(results)
