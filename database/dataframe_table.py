import pandas as pd
import os
from MetricsTracker import cost_tracker
from database import dataframe_table

class DataFrameTable(pd.DataFrame):

    def __init__(self, *args, **kwargs):
        super(DataFrameTable, self).__init__(*args, **kwargs)

    @property
    def _constructor(self):
        return DataFrameTable

    def __setitem__(self, key, value):
        if isinstance(value, dataframe_table.DataFrameTable) and cost_tracker.ObliviousTracker.get_flag():
            for accessed_index in range(key.start, key.stop):
                cost_tracker.ObliviousTracker.register_cost(1, False, accessed_index)
                cost_tracker.ObliviousTracker.register_memory_access(accessed_index, False)
        else:
            cost_tracker.ObliviousTracker.register_cost(1, False, key)
            cost_tracker.ObliviousTracker.register_memory_access(key, False)
        super(DataFrameTable, self).__setitem__(key, value)

    def __getitem__(self, key):
        result =  super(DataFrameTable, self).__getitem__(key)
        if isinstance(result, dataframe_table.DataFrameTable) and cost_tracker.ObliviousTracker.get_flag():
            for accessed_index in range(key.start, key.stop):
                cost_tracker.ObliviousTracker.register_cost(1, True, accessed_index)
                cost_tracker.ObliviousTracker.register_memory_access(accessed_index, True)
        else:
            cost_tracker.ObliviousTracker.register_cost(1, True, key)
            cost_tracker.ObliviousTracker.register_memory_access(key, True)
        return result