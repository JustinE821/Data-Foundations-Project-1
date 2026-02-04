from pathlib import Path
import pandas as pd

script_dir_path = Path(__file__).parent.resolve()
csv_files_dir_relative_path = script_dir_path / '..' / '..' / 'data'

# The above relative path solution comes from: https://stackoverflow.com/questions/40416072/reading-a-file-using-a-relative-path-in-a-python-project

national_df = pd.read_csv(csv_files_dir_relative_path / 'National_Interagency_Fire_Occurrence_Sixth_Edition_1992-2020_(Feature_Layer).csv')

new_york_df = pd.read_csv(csv_files_dir_relative_path / 'New_York_State_Forest_Ranger_Wildland_Fire_Reporting_Database__Beginning_2008.csv')

oregon_df = pd.read_csv(csv_files_dir_relative_path / 'ODF_Fire_Occurrence_Data_2000-2022.csv')

california_df = pd.read_csv(csv_files_dir_relative_path / 'California_Historic_Fire_Perimeters_-6236829869961296710.csv')

