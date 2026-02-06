"""
This file contains the functions for computing missing columns and renaming columns
to transform our DataFrames for each dataset into a standard form.
"""
import pandas as pd
from data_processing.constants import STANDARD_COLUMN_NAMES
from data_processing.constants import NATIONAL_COLS, NEW_YORK_COLS, OREGON_COLS, CALIFORNIA_COLS

def compute_date_cols(column_list):
    return [column for column in column_list if "date" in column.lower()]

def convert_datetime_to_date(df, date_columns):
    df[date_columns[0]] = df[date_columns[0]].dt.date
    df[date_columns[1]] = df[date_columns[1]].dt.date

def create_size_class(df, ACREAGE_COL):
    acreage = df[ACREAGE_COL]
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