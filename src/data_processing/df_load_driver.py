"""
This file coordinates the execution of step 1 in our data processing pipeline. That is, loading 
the necessary columns of the raw data into DataFrames, performing validation and converting the 
data into a standard form.
"""
import pandas as pd
from data_processing.constants import NATIONAL_PATH, NEW_YORK_PATH, OREGON_PATH, CALIFORNIA_PATH
from data_processing.constants import NATIONAL_COLS, NEW_YORK_COLS, OREGON_COLS, CALIFORNIA_COLS
from data_processing.constants import STANDARD_COLUMN_NAMES

def compute_date_cols(column_list):
    return [column for column in column_list if "date" in column.lower()]

def convertDatetimeToDate(df, date_columns):
    df[date_columns[0]] = df[date_columns[0]].dt.date
    df[date_columns[1]] = df[date_columns[1]].dt.date


def get_data_frames():
    national_date_columns = compute_date_cols(NATIONAL_COLS)
    new_york_date_columns = compute_date_cols(NEW_YORK_COLS)
    oregon_date_columns = compute_date_cols(OREGON_COLS)
    california_date_columns = compute_date_cols(CALIFORNIA_COLS)

    national_converters = {}
    new_york_converters = {}
    oregon_converters = {}
    california_converters = {}


    national_df = pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, nrows=20, parse_dates=national_date_columns, converters=national_converters)
    new_york_df = pd.read_csv(NEW_YORK_PATH, usecols=NEW_YORK_COLS, nrows=5, parse_dates=new_york_date_columns, converters=new_york_converters)
    oregon_df = pd.read_csv(OREGON_PATH, usecols=OREGON_COLS, nrows=5, parse_dates=oregon_date_columns, converters=oregon_converters)
    california_df = pd.read_csv(CALIFORNIA_PATH, usecols=CALIFORNIA_COLS, nrows=5, parse_dates=california_date_columns, converters=california_converters)

    # Convert datetime columns into date columns
    # New York dataset is already in date format
    convertDatetimeToDate(national_df, national_date_columns)
    convertDatetimeToDate(oregon_df, oregon_date_columns)
    convertDatetimeToDate(california_df, california_date_columns)

    return [national_df, new_york_df, oregon_df, california_df]