"""
This file is responsible for providing converter functions that convert raw data from
columns in the original datasets into the standard format. 
"""

from data_processing.constants import cause_dict
#from data_processing.constants import STANDARD_COLUMN_NAMES
def cause_converter(cause):
    # for entry in cause:
    #     entry = cause_dict[entry]

    return cause_dict[cause]