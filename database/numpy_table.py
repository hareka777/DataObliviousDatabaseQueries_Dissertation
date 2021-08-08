import numpy as np
from MetricsTracker import cost_tracker

# subclassing implementation based on this source:
# https://numpy.org/doc/stable/user/basics.subclassing.html
class NumpyTable(np.ndarray):
    def __new__(subtype, shape, dtype=float, buffer=None, offset=0,
                strides=None, order=None):
        # Create the ndarray instance of our type, given the usual
        # ndarray input arguments.  This will call the standard
        # ndarray constructor, but return an object of our type.
        # It also triggers a call to InfoArray.__array_finalize__
        instance = super().__new__(subtype, shape, dtype,
                              buffer, offset, strides, order)
        # Finally, we must return the newly created object:
        return instance

    def __setitem__(self, key, value):
        number_of_columns = self.shape[1]
        index_of_accessed_cell = key[0] * number_of_columns + key[1]
        cost_tracker.ObliviousTracker.register_cost(1, False, index_of_accessed_cell)
        cost_tracker.ObliviousTracker.register_memory_access(index_of_accessed_cell, False)
        #print('Numpy tracking - set')
        super(NumpyTable, self).__setitem__(key, value)


    def __getitem__(self, key):
        #print('Accessed item: ',key)
        if cost_tracker.ObliviousTracker.get_flag():
            number_of_columns = self.shape[1]
            index_of_accessed_cell = key[0] * number_of_columns + key[1]
            cost_tracker.ObliviousTracker.register_cost(1, True, index_of_accessed_cell)
            cost_tracker.ObliviousTracker.register_memory_access(index_of_accessed_cell, True)
        #print('Numpy tracking - get')
        return super(NumpyTable, self).__getitem__(key)