"""
This file is responsible for providing converter functions that convert raw data from
columns in the original datasets into the standard format. 
"""

from data_processing.constants import cause_dict

#Function remaps the cause of a wildfire to reflect the database format used
def cause_converter(cause):
    if cause not in cause_dict:
        return 11
    return cause_dict[cause]
