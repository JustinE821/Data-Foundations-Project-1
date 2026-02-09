"""
This file coordinates the execution of step 1 in our data processing pipeline. That is, loading 
the necessary columns of the raw data into DataFrames, performing validation and converting the 
data into a standard form.
"""
import pandas as pd
from data_processing.constants import NATIONAL_PATH, NEW_YORK_PATH, OREGON_PATH, CALIFORNIA_PATH
from data_processing.constants import NATIONAL_COLS, NEW_YORK_COLS, OREGON_COLS, CALIFORNIA_COLS
from data_processing.converter_functions import cause_converter
from data_processing.standardize_columns import compute_date_cols, convert_datetime_to_date, create_size_class, standardize_column_names

def get_dataframes():
    national_date_columns = compute_date_cols(NATIONAL_COLS)
    new_york_date_columns = compute_date_cols(NEW_YORK_COLS)
    oregon_date_columns = compute_date_cols(OREGON_COLS)
    california_date_columns = compute_date_cols(CALIFORNIA_COLS)

    national_converters = {'NWCG_GENERAL_CAUSE': cause_converter}
    new_york_converters = {'Cause': cause_converter}
    oregon_converters = {'GeneralCause': cause_converter}
    california_converters = {'Cause': cause_converter}

    national_list = []
    for chunk in pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, parse_dates=national_date_columns, date_format="%Y/%m/%d %H:%M:%S+00", converters=national_converters, engine="c", chunksize=10000):
        national_list.append(chunk)
    national_df = pd.concat(national_list, ignore_index=True)
    new_york_df = pd.read_csv(NEW_YORK_PATH, usecols=NEW_YORK_COLS, parse_dates=new_york_date_columns, converters=new_york_converters)
    oregon_df = pd.read_csv(OREGON_PATH, usecols=OREGON_COLS, parse_dates=oregon_date_columns, date_format="%m/%d/%Y %I:%M:%S %p", converters=oregon_converters)
    california_df = pd.read_csv(CALIFORNIA_PATH, usecols=CALIFORNIA_COLS, parse_dates=california_date_columns, converters=california_converters)

    # Obtain the fire size classification for each entry
    new_york_df['size_class'] = create_size_class(new_york_df, 'Acreage')
    california_df['size_class'] = create_size_class(california_df, 'GIS Calculated Acres')

    # Convert datetime columns into date columns
    # New York dataset is already in date format
    convert_datetime_to_date(national_df, national_date_columns)
    convert_datetime_to_date(oregon_df, oregon_date_columns)
    convert_datetime_to_date(california_df, california_date_columns)

    # Fill simple missing columns
    california_df['longitude'] = None
    california_df['latitude'] = None
    new_york_df['state'] = 'NY'
    oregon_df['state'] = 'OR'

    [national_df, new_york_df, oregon_df, california_df] = standardize_column_names(national_df=national_df, new_york_df=new_york_df, oregon_df=oregon_df, california_df=california_df)

    return [national_df, new_york_df, oregon_df, california_df]
