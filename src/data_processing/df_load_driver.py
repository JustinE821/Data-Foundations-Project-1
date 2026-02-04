import pandas as pd
from pathlib import Path

# Paths to datasets
SCRIPT_DIR_PATH = Path(__file__).parent.resolve()
CSV_FILES_DIR_RELATIVE_PATH = SCRIPT_DIR_PATH / '..' / '..' / 'data'
NATIONAL_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'National_Interagency_Fire_Occurrence_Sixth_Edition_1992-2020_(Feature_Layer).csv'
NEW_YORK_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'New_York_State_Forest_Ranger_Wildland_Fire_Reporting_Database__Beginning_2008.csv'
OREGON_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'ODF_Fire_Occurrence_Data_2000-2022.csv'
CALIFORNIA_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'California_Historic_Fire_Perimeters_-6236829869961296710.csv'

# Standard column names that we will convert DataFrames into
STANDARD_COLUMN_NAMES = ['longitude', 'latitude', 'state', 'fire_name', 'report_date', 'containment_date', 'cause', 'acreage', 'size_class']

# Columns to load per dataset
NATIONAL_COLS = ['LONGITUDE', 'LATITUDE', 'STATE', 'FIRE_NAME', 'DISCOVERY_DATE', 'CONT_DATE', 'NWCG_GENERAL_CAUSE', 'FIRE_SIZE', 'FIRE_SIZE_CLASS']
# Note: New York dataset does not have columns for state or size_class
# We will have to insert a state column with 'NY' for its values
# We will have to calculate the size class for each record based on its acreage 
NEW_YORK_COLS = ['Longitude', 'Latitude', 'Incident Name', 'Fire Start Date', 'Initial Report Date', 'Fire Out Date', 'Cause', 'Acreage'] 
# Note: Oregon dataset does not have a state column
# We will have to insert a state column with 'OR' for its values
OREGON_COLS = ['Long_DD', 'Lat_DD', 'FireName', 'ReportDateTime', 'Control_DateTime', 'GeneralCause', 'EstTotalAcres', 'Size_class']
# Note: California dataset does not have longitude, latitude, and size_class columns
# We will have to insert longitude and latitude columns with null values
# We will have to calculate the size class for each record based on its acreage
CALIFONIA_COLS = ['State', 'Fire Name', 'Alarm Date', 'Containment Date', 'Cause', 'GIS Calculated Acres']


def get_data_frames():
    national_df = pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, nrows=5)
    new_york_df = pd.read_csv(NEW_YORK_PATH, usecols=NEW_YORK_COLS, nrows=5)
    oregon_df = pd.read_csv(OREGON_PATH, usecols=OREGON_COLS, nrows=5)
    california_df = pd.read_csv(CALIFORNIA_PATH, usecols=CALIFONIA_COLS, nrows=5)

    return [national_df, new_york_df, oregon_df, california_df]