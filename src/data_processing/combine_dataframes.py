"""
This file is responsible for step 2 of our data processing pipeline. It takes in all of the 
standardized tables and concatenates them into one big table. Then, based on the candidate key
of fire_name, report_date, and state, it drops duplicates. The dropped duplicates should be 
written to log files.
"""
import pandas as pd
import numpy as np
from data_processing.standardize_columns import limit_date_range
from pathlib import Path
import logging

KEY = ['state_id', 'report_date', 'fire_name']

log_path = Path(__file__).parent.resolve() / '..' / '..' / 'logs' / 'dropped.log'
logging.basicConfig(
     filename=log_path, 
     level=logging.DEBUG,
     format="%(asctime)s | %(levelname)s | %(message)s", 
)
logger = logging.getLogger(__name__)

def log_null_key(df):
    null_key_mask = df[KEY].isna().any(axis=1)
    logger.info(f"\n{df[null_key_mask].to_string()}")
        
def log_duplicate_key(df):
    duplicates = df[df.duplicated(subset=KEY)]
    logger.info(f"\n{duplicates.to_string()}")

def combine_dataframes(standardized_dataframe_list):
    combined_df = pd.concat(standardized_dataframe_list, axis=0, join='inner', ignore_index=True)
    log_null_key(combined_df)
    combined_df = combined_df.dropna(subset=KEY)
    log_duplicate_key(combined_df)
    combined_df = combined_df.drop_duplicates(subset=KEY, ignore_index=True)
    combined_df = combined_df.replace({np.nan: None})
    combined_df = combined_df.rename_axis('wildfire_id').reset_index()
    return combined_df
