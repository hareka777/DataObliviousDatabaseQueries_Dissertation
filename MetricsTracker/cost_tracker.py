class SparkTracker():

    __execution_times = []
    __costs = []
    __result_table_sizes = []

    @classmethod
    def register_cost(cls, total, read, write):
        cls.__costs.append([total, read, write])

    @classmethod
    def register_execution_time(cls, duration):
        cls.__execution_times.append(duration)

    @classmethod
    def register_result_table_size(cls, result_table_size):
        cls.__result_table_sizes.append(result_table_size)

    @classmethod
    def log_experiment_results(cls, filename):
        result_table_lengths = len(cls.__result_table_sizes)
        experiment_length = len(cls.__costs)
        with open(filename + '_costs', 'w') as file_handler:
            for index in range(experiment_length):
                file_handler.write("{}\n".format(cls.__costs[index]))

        if len(cls.__execution_times) > 0:
            with open(filename + '_execution_times', 'w') as file_handler:
                for index in range(len(cls.__execution_times)):
                    file_handler.write("{}\n".format(cls.__execution_times[index]))

        if result_table_lengths > 0:
            with open(filename + '_result_table_sizes', 'w') as file_handler:
                for index in range(result_table_lengths):
                    file_handler.write("{}\n".format(cls.__result_table_sizes[index]))

class ObliviousTracker():
    __current_cost_read = 0
    __current_cost_write = 0
    __tracking_enabled_flag = False
    __memory_patterns = []
    __current_memory_patterns = []
    __last_read = False
    __block_size = 1
    __execution_times = []
    __all_costs = []
    __last_read_location = None
    __last_written_location = None
    __result_table_sizes = []

    def __init__(self):
        self.__tracking_enabled_flag = False

    @classmethod
    def register_cost(cls, partial_cost, read, accessed_element):
        if cls.__tracking_enabled_flag:
            if read:
                if cls.__last_read_location is None or int(accessed_element) >= cls.__last_read_location:
                    if cls.__last_read_location is None or (cls.__last_read_location + partial_cost == accessed_element and accessed_element % cls.__block_size == 0):
                        cls.__current_cost_read += partial_cost
                else:
                    cls.__current_cost_read += partial_cost
            else:
                if cls.__last_written_location is None or int(accessed_element) >= cls.__last_written_location:
                    if cls.__last_written_location is None or (cls.__last_written_location + partial_cost == accessed_element and accessed_element % cls.__block_size == 0):
                        cls.__current_cost_write += partial_cost
                else:
                    cls.__current_cost_write += partial_cost

    @classmethod
    def set_block_size(cls, block_size):
        cls.__block_size = block_size

    @classmethod
    def register_result_table_size(cls, result_table_size):
        cls.__result_table_sizes.append(result_table_size)

    @classmethod
    def register_memory_access(cls, accessed_memory_location, read):
        if cls.__tracking_enabled_flag:
            if read:
                suffix = '_read'
                cls.__last_read = True
                cls.__last_read_location = int(accessed_memory_location)
            else:
                suffix = '_write'
                cls.__last_read = False
                cls.__last_written_location = int(accessed_memory_location)
            cls.__current_memory_patterns.append(str(accessed_memory_location) + suffix)

    @classmethod
    def register_execution_time(cls, duration):
        cls.__execution_times.append(duration)

    @classmethod
    def query_completed(cls):
        print(cls.__current_cost_read + cls.__current_cost_write)
        print(cls.__current_cost_read)
        cls.__all_costs.append([cls.__current_cost_read + cls.__current_cost_write, cls.__current_cost_read, cls.__current_cost_write])
        if len(cls.__current_memory_patterns) > 0:
            cls.__memory_patterns.append(cls.__current_memory_patterns)
            cls.__current_memory_patterns = []
        cls.__current_cost_read = 0
        cls.__current_cost_write = 0
        cls.__tracking_enabled_flag = False
        cls.__last_read = False
        cls.__last_read_location = None
        cls.__last_written_location = None

    @classmethod
    def set_tracking_enabled_flag(cls, flag):
        cls.__tracking_enabled_flag = flag

    @classmethod
    def log_experiment_results(cls, filename):
        experiment_length = len(cls.__all_costs)
        memorry_access_pattern_length = len(cls.__memory_patterns)
        result_table_lengths = len(cls.__result_table_sizes)
        execution_time_length = \
            len(cls.__execution_times)
        with open(filename + '_costs', 'w') as file_handler:
            for index in range(experiment_length):
                file_handler.write("{}\n".format(cls.__all_costs[index]))
        if memorry_access_pattern_length > 0 :
            with open(filename + '_access_patterns', 'w') as file_handler:
                for index in range(memorry_access_pattern_length):
                    file_handler.write("{}\n".format(cls.__memory_patterns[index]))
        if execution_time_length > 0:
            with open(filename + '_execution_times', 'w') as file_handler:
                for index in range(execution_time_length):
                    file_handler.write("{}\n".format(cls.__execution_times[index]))
        if result_table_lengths > 0:
            with open(filename + '_result_table_sizes', 'w') as file_handler:
                for index in range(result_table_lengths):
                    file_handler.write("{}\n".format(cls.__result_table_sizes[index]))
        cls.__tracking_enabled_flag = False
        cls.__all_costs = []
        cls.__memory_patterns = []
        cls.__execution_times = []


    @classmethod
    def get_flag(cls):
        return cls.__tracking_enabled_flag