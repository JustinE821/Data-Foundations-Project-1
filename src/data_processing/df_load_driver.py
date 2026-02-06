"""
This file coordinates the execution of step 1 in our data processing pipeline. That is, loading 
the necessary columns of the raw data into DataFrames, performing validation and converting the 
data into a standard form.
"""
import pandas as pd
from data_processing.constants import NATIONAL_PATH, NEW_YORK_PATH, OREGON_PATH, CALIFORNIA_PATH
from data_processing.constants import NATIONAL_COLS, NEW_YORK_COLS, OREGON_COLS, CALIFORNIA_COLS
from data_processing.constants import STANDARD_COLUMN_NAMES
from data_processing.converter_functions import cause_converter

def compute_date_cols(column_list):
    return [column for column in column_list if "date" in column.lower()]

def convertDatetimeToDate(df, date_columns):
    df[date_columns[0]] = df[date_columns[0]].dt.date
    df[date_columns[1]] = df[date_columns[1]].dt.date

def create_size_class(df, ACREAGE_COL):
    acreage = df[ACREAGE_COL]
    print(len(acreage))
    size_class_list = list()
    for i in range(len(acreage)):
        entry = acreage[i]
        if entry < 0.26:
            size_class_list.append('A')
        elif entry < 10.0:
            size_class_list.append('B')
        elif entry < 100.0:
            size_class_list.append('C')
        elif entry < 300.0:
            size_class_list.append('D')
        elif entry < 1000.0:
            size_class_list.append('E')
        elif entry < 5000.0:
            size_class_list.append('F')
        else:
            size_class_list.append('G')

    size_class = pd.Series(size_class_list)
    return size_class



def get_data_frames():
    national_date_columns = compute_date_cols(NATIONAL_COLS)
    new_york_date_columns = compute_date_cols(NEW_YORK_COLS)
    oregon_date_columns = compute_date_cols(OREGON_COLS)
    california_date_columns = compute_date_cols(CALIFORNIA_COLS)

    national_converters = {'NWCG_GENERAL_CAUSE': cause_converter}
    new_york_converters = {'Cause': cause_converter} #  ('Acreage', 'Size Class'): size_converter
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
    new_york_df['Size Class'] = create_size_class(new_york_df, 'Acreage')
    california_df['Size Class'] = create_size_class(california_df, 'GIS Calculated Acres')

    # Convert datetime columns into date columns
    # New York dataset is already in date format
    convertDatetimeToDate(national_df, national_date_columns)
    convertDatetimeToDate(oregon_df, oregon_date_columns)
    convertDatetimeToDate(california_df, california_date_columns)

    return [national_df, new_york_df, oregon_df, california_df]