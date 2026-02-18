"""
This file contains the functions for computing missing columns and renaming columns
to transform our DataFrames for each dataset into a standard form.
"""
import datetime
import pandas as pd
from data_processing.constants import NATIONAL_TO_STANDARD_COLUMN_MAPPING, NEW_YORK_TO_STANDARD_COLUMN_MAPPING
from data_processing.constants import OREGON_TO_STANDARD_COLUMN_MAPPING, CALIFORNIA_TO_STANDARD_COLUMN_MAPPING

def compute_date_cols(column_list):
    return [column for column in column_list if "date" in column.lower()]

#Converts datetime to date
def convert_datetime_to_date(df, date_columns):
    for date_col in date_columns:
        if isinstance(df[date_col].iloc[0], datetime.datetime):
            df[date_col] = df[date_col].dt.date

#This function removes entries from datasets which are not from the 2010s
def limit_date_range(df):
    START_DATE = datetime.date(2010, 1, 1)
    END_DATE = datetime.date(2019, 12, 31)
    df = df[(df['report_date'] >= START_DATE) & (df['report_date'] <= END_DATE)]
    
    return df

#This function generates a size_class column for datasets which do not have it
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
        elif entry < 1000000.0:
            size_class_list.append('G')
        else:
            size_class_list.append(None)

    size_class = pd.Series(size_class_list)
    return size_class

#Remaps dataset column names to reflect the standardized structure
def standardize_column_names(national_df, new_york_df, oregon_df, california_df):
    national_df = national_df.rename(columns=NATIONAL_TO_STANDARD_COLUMN_MAPPING)
    new_york_df = new_york_df.rename(columns=NEW_YORK_TO_STANDARD_COLUMN_MAPPING)
    oregon_df = oregon_df.rename(columns=OREGON_TO_STANDARD_COLUMN_MAPPING)
    california_df = california_df.rename(columns=CALIFORNIA_TO_STANDARD_COLUMN_MAPPING)
    return [national_df, new_york_df, oregon_df, california_df]
    