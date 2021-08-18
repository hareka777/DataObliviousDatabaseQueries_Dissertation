import matplotlib. pyplot as plt
import numpy as np

# experiment parameters

block_sizes = [1, 3, 5, 10, 20]
oblivious_memory_sizes = [0.0015, 0.0075, 0.015, 0.03 ,0.045]

# ObliDB Hash join overall costs
oblidb_hash_all_costs = {}
oblidb_hash_reading_costs = {}
oblidb_hash_writing_costs = {}

algorithms = ['ObliDB Hash Join', 'ObliDB 0-OM join', 'Opaque sort-merge join']

oblidb_hash_all_costs[str(0.0015)] = [26100, 8728, 5254, 2648, 1345]
oblidb_hash_reading_costs[str(0.0015)] = [13200, 4428, 2674, 1358, 700]
oblidb_hash_writing_costs[str(0.0015)] = [12900, 4300, 2580, 1290, 645]

oblidb_hash_all_costs[str(0.0075)] = [5700, 1906, 1140, 574, 291]
oblidb_hash_reading_costs[str(0.0075)] = [3000, 1006, 600, 304, 156]
oblidb_hash_writing_costs[str(0.0075)] = [2700, 900, 540, 270, 135]

oblidb_hash_all_costs[str(0.015)] = [3300, 1103, 660, 330, 167]
oblidb_hash_reading_costs[str(0.015)] = [1800, 603, 360, 180, 92]
oblidb_hash_writing_costs[str(0.015)] = [1500, 500, 300, 150, 75]

oblidb_hash_all_costs[str(0.03)] = [2100, 702, 420, 210, 167]
oblidb_hash_reading_costs[str(0.03)] = [1200, 402, 240, 120, 92]
oblidb_hash_writing_costs[str(0.03)] = [900, 300, 180, 90, 75]

oblidb_hash_all_costs[str(0.045)] = [1500, 500, 300, 150, 76]
oblidb_hash_reading_costs[str(0.045)] = [900, 300, 180, 90, 46]
oblidb_hash_writing_costs[str(0.045)] = [600, 200, 120, 60, 30]

# ObliDB OM join overall costs
oblidb_om_all_costs = {}
oblidb_om_reading_costs = {}
oblidb_om_writing_costs = {}

oblidb_om_all_costs[str(0.0015)] = [114477, 112693, 112319, 112121, 112023]
oblidb_om_reading_costs[str(0.0015)] = [57537, 56447, 56219, 56090, 56026]
oblidb_om_writing_costs[str(0.0015)] = [56940, 56246, 56100, 56031, 55997]

oblidb_om_all_costs[str(0.0075)] = [107431, 106027, 105745, 105547, 105449]
oblidb_om_reading_costs[str(0.0075)] = [54014, 53114, 52932, 52803, 52739]
oblidb_om_writing_costs[str(0.0075)] = [53417, 52913, 52813, 52744, 52710]

oblidb_om_all_costs[str(0.015)] = [104871, 103517, 103245, 103047, 102949]
oblidb_om_reading_costs[str(0.015)] = [52734, 51859, 51682, 51553, 51489]
oblidb_om_writing_costs[str(0.015)] = [52137, 51658, 1563, 51494, 51460]

oblidb_om_all_costs[str(0.03)] = [102823, 101493, 101221, 101023, 100925]
oblidb_om_reading_costs[str(0.03)] = [51710, 50847, 50670, 50541, 50477]
oblidb_om_writing_costs[str(0.03)] = [51113, 50646, 50551, 50482, 50448]

oblidb_om_all_costs[str(0.045)] = [101287, 99963, 99697, 99499, 99401]
oblidb_om_reading_costs[str(0.045)] = [50942, 50082, 49908, 49779, 49715]
oblidb_om_writing_costs[str(0.045)] = [50345, 49881, 49789, 49720, 49686]

# Opaque's sort merge join overall costs
opaque_all_costs = {}
opaque_reading_costs = {}
opaque_writing_costs = {}

opaque_all_costs[str(0.0015)] = [191592, 190769, 190607, 190517, 190473]
opaque_reading_costs[str(0.0015)] = [95498, 95285, 95244, 95229, 95222]
opaque_writing_costs[str(0.0015)] = [96094, 95484, 95363, 95288, 95251]

opaque_all_costs[str(0.0075)] = [133070, 132483, 132367, 132285, 132245]
opaque_reading_costs[str(0.0075)] = [66237, 66142, 66124, 66113, 66108]
opaque_writing_costs[str(0.0075)] = [66833, 66341, 66243, 66172, 66137]

opaque_all_costs[str(0.015)] = [114954, 114409, 114297, 114217 ,114177]
opaque_reading_costs[str(0.015)] = [57179, 57105, 57089, 57079, 57074]
opaque_writing_costs[str(0.015)] = [57775, 57304, 57208, 57138, 57103]

opaque_all_costs[str(0.03)] = [3714, 1322, 829, 469, 296]
opaque_reading_costs[str(0.03)] = [2405, 811, 482, 242, 123]
opaque_writing_costs[str(0.03)] = [1309, 511, 347, 227, 173]

opaque_all_costs[str(0.045)] = [3762, 1357, 877, 522, 344]
opaque_reading_costs[str(0.045)] = [2405, 802, 482, 243, 127]
opaque_writing_costs[str(0.045)] = [1357, 555, 395, 279, 217]

def generate_chart_1(opaque_all, opaque_reading, oblidb_om_all, oblidb_om_reading,
                     oblidb_hash_all, oblidb_hash_reading, memory_size):
    block_sizes = ['1 record','3 records', '5 records', '10 records', '20 records']

    column_width = 0.2

    positions_1 = np.arange(5)
    positions_2 = [previous_width + column_width for previous_width in positions_1]
    positions_3 = [previous_width + column_width for previous_width in positions_2]

    plt.figure(figsize=[10, 7])
    # I have choose the colours from the following website:
    # https://www.color-hex.com/
    plt.bar(positions_1, opaque_all, color='#aa77aa', width=column_width, edgecolor='white', label='Opaque sort-merge join algorithm')
    plt.bar(positions_1, opaque_reading,color='#a7a7a7', width=column_width, edgecolor='white', label='Reading cost')
    plt.bar(positions_2, oblidb_om_all, color='#46282a', edgecolor='white', width=column_width, label='ObliDB 0-OM algorithm')
    plt.bar(positions_2, oblidb_om_reading, color='#a7a7a7', edgecolor='white', width=column_width)
    plt.bar(positions_3, oblidb_hash_all, color='#117791', edgecolor='white', width=column_width, label='ObliDB hash join algorithm')
    plt.bar(positions_3, oblidb_hash_reading, color='#a7a7a7', edgecolor='white', width=column_width)

    plt.xlabel('Block size', fontsize=20)
    plt.ylabel('Cost [Number of memory accesses]', fontsize=20)
    plt.yscale('log', base=2)
    plt.yticks(fontsize=20)
    plt.xticks([r + column_width for r in range(len(positions_1))], block_sizes, fontsize=20)
    plt.title('Cost of join algorithms (memory size= '+ str(memory_size) + 'MB )', fontsize=25)

    # the legend style implementation based on the following tutorial/documentation:
    # https://matplotlib.org/stable/tutorials/intermediate/legend_guide.html
    plt.legend(bbox_to_anchor=(0., 1.2, 1., .102), loc='upper left',
           ncol=2, mode="expand", borderaxespad=0., fontsize=17)
    plt.show()

for memory in oblivious_memory_sizes:
    '''for block_size_index in range(len(block_sizes)):
        reading_costs = [reading_costs_opaque_filtering[block_size_index], oblidb_small_reading_costs[str(memory)][block_size_index],
                         oblidb_large_reading_costs[str(memory)][block_size_index], oblidb_hash_reading_costs[str(memory)][block_size_index]]
        writing_costs = [writing_costs_opaque_filtering[block_size_index], oblidb_small_writing_costs[str(memory)][block_size_index],
                         oblidb_large_writing_costs[str(memory)][block_size_index], oblidb_hash_writing_costs[str(memory)][block_size_index]]
        all_costs = [all_costs_opaue_filtering[block_size_index], oblidb_small_all_costs[str(memory)][block_size_index],
                         oblidb_large_all_costs[str(memory)][block_size_index], oblidb_hash_all_costs[str(memory)][block_size_index]]
        generate_chart_1(reading_costs, writing_costs, all_costs, block_sizes[block_size_index], memory)'''
    generate_chart_1(opaque_all_costs[str(memory)], opaque_reading_costs[str(memory)], oblidb_om_all_costs[str(memory)],
                     oblidb_om_reading_costs[str(memory)], oblidb_hash_all_costs[str(memory)], oblidb_hash_reading_costs[str(memory)],
                      memory)
