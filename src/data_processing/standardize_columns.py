"""
This file contains the functions for computing missing columns and renaming columns
to transform our DataFrames for each dataset into a standard form.
"""
import pandas as pd
from data_processing.constants import NATIONAL_TO_STANDARD_COLUMN_MAPPING, NEW_YORK_TO_STANDARD_COLUMN_MAPPING
from data_processing.constants import OREGON_TO_STANDARD_COLUMN_MAPPING, CALIFORNIA_TO_STANDARD_COLUMN_MAPPING

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

def standardize_column_names(national_df, new_york_df, oregon_df, california_df):
    national_df = national_df.rename(columns=NATIONAL_TO_STANDARD_COLUMN_MAPPING)
    new_york_df = new_york_df.rename(columns=NEW_YORK_TO_STANDARD_COLUMN_MAPPING)
    oregon_df = oregon_df.rename(columns=OREGON_TO_STANDARD_COLUMN_MAPPING)
    california_df = california_df.rename(columns=CALIFORNIA_TO_STANDARD_COLUMN_MAPPING)
    