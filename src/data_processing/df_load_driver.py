import pandas as pd
from data_processing.constants import NATIONAL_PATH, NEW_YORK_PATH, OREGON_PATH, CALIFORNIA_PATH
from data_processing.constants import NATIONAL_COLS, NEW_YORK_COLS, OREGON_COLS, CALIFONIA_COLS
from data_processing.constants import STANDARD_COLUMN_NAMES

def get_data_frames():
    national_df = pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, nrows=5)
    new_york_df = pd.read_csv(NEW_YORK_PATH, usecols=NEW_YORK_COLS, nrows=5)
    oregon_df = pd.read_csv(OREGON_PATH, usecols=OREGON_COLS, nrows=5)
    california_df = pd.read_csv(CALIFORNIA_PATH, usecols=CALIFONIA_COLS, nrows=5)

    return [national_df, new_york_df, oregon_df, california_df]