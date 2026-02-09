"""
This file implements step 3, breaking the combined DataFrame into DataFrames corresponding to 
database tables. 
"""
from data_processing.constants import WILDFIRE_COLUMN_NAMES, WILDFIRE_SIZE_COLUMN_NAMES, WILDFIRE_LOCATION_COLUMN_NAMES

def split_dataframes(combined_df):
    combined_df = combined_df.rename(columns={'cause': 'cause_id'})
    wildfire_df = combined_df[WILDFIRE_COLUMN_NAMES]
    wildfire_size_df = combined_df[WILDFIRE_SIZE_COLUMN_NAMES]
    wildfire_location_df = combined_df[WILDFIRE_LOCATION_COLUMN_NAMES]
    return [wildfire_df, wildfire_size_df, wildfire_location_df]