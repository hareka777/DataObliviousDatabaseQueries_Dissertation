import os

def check_if_memory_access_patterns_are_the_same(file_path):
    '''
    Checking if the memory accesses are the same in the given query types in order to test data obliviousness
    file_path: the path of the file that contains the memory access patterns relative to the current working directory
    :return: boolean, depends on if the memory access patterns are identical
    '''
    access_patterns = None
    with open(os.getcwd() + "/Results/"+ file_path, "r") as access_pattern_file:
        access_patterns = access_pattern_file.readlines()

    oblivious_flag = access_patterns.count(access_patterns[0]) == len(access_patterns)
    print('The access patterns are the same: ', oblivious_flag)

check_if_memory_access_patterns_are_the_same('Opaque/Filtering/opaque_filtering_block_1.txt _access_patterns')