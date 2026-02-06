"""
This file is responsible for step 2 of our data processing pipeline. It takes in all of the 
standardized tables and concatenates them into one big table. Then, based on the candidate key
of fire_name, report_date, and state, it drops duplicates. The dropped duplicates should be 
written to log files.
"""
import pandas as pd

def combine_dataframes(standardized_dataframe_list):
    combined_df = pd.concat(standardized_dataframe_list, axis=0, join='inner', ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['state', 'report_date', 'fire_name'])
    combined_df = combined_df.rename_axis('wildfire_id').reset_index()
    return combined_df
