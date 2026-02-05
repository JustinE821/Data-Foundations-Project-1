# """
# The purpose of this file is to provide a convenient area for debugging.
# """
# from pathlib import Path
# import pandas as pd

# script_dir_path = Path(__file__).parent.resolve()
# csv_files_dir_relative_path = script_dir_path / '..' / '..' / 'data'

# # The above relative path solution comes from: https://stackoverflow.com/questions/40416072/reading-a-file-using-a-relative-path-in-a-python-project

# national_df = pd.read_csv(csv_files_dir_relative_path / 'National_Interagency_Fire_Occurrence_Sixth_Edition_1992-2020_(Feature_Layer).csv')

# new_york_df = pd.read_csv(csv_files_dir_relative_path / 'New_York_State_Forest_Ranger_Wildland_Fire_Reporting_Database__Beginning_2008.csv')

# oregon_df = pd.read_csv(csv_files_dir_relative_path / 'ODF_Fire_Occurrence_Data_2000-2022.csv')

# california_df = pd.read_csv(csv_files_dir_relative_path / 'California_Historic_Fire_Perimeters_-6236829869961296710.csv')

"""
The purpose of this file is to provide a convenient area for debugging.
"""
from ..data_processing.constants import NATIONAL_PATH, NATIONAL_COLS, NEW_YORK_PATH, NEW_YORK_COLS, OREGON_PATH, OREGON_COLS, CALIFORNIA_PATH, CALIFORNIA_COLS
from ..data_processing.converter_functions import cause_converter
import pandas as pd


# chunk_list = []
# for chunk in pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, chunksize=10000):
#     chunk_list.append(chunk)

# national_df = pd.concat(chunk_list, axis=0)
#national_df = pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, engine='python', on_bad_lines='skip')
#trouble_row = pd.read_csv(NATIONAL_PATH, skiprows=425980, nrows=10)
#national_df = pd.read_csv(NATIONAL_PATH, usecols=NATIONAL_COLS, nrows=425985)

#print(trouble_row.columns)
new_york_df = pd.read_csv(NEW_YORK_PATH, usecols=NEW_YORK_COLS)

cause_converter(new_york_df["Cause"])

# oregon_df = pd.read_csv(OREGON_PATH, usecols=OREGON_COLS)

# california_df = pd.read_csv(CALIFORNIA_PATH, usecols=CALIFORNIA_COLS)

# unique_causes_ndf = national_df['NWCG_GENERAL_CAUSE'].unique()
# print(unique_causes_ndf)
# natural_causes_ndf = national_df[national_df['NWCG_GENERAL_CAUSE'] == "Natural"]
# print(natural_causes_ndf)

print("-----------------------")
#print(len(unique_causes_ndf))

unique_causes_nydf = new_york_df['Cause'].unique()
print(unique_causes_nydf)

# unique_causes_ordf = oregon_df['GeneralCause'].unique()
# print(unique_causes_ordf)



##425985 broken row