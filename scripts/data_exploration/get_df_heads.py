from pathlib import Path
import pandas as pd

script_dir_path = Path(__file__).parent.resolve()
csv_files_dir_relative_path = script_dir_path / '..' / '..' / 'resources' / 'csv_files'

# The above relative path solution comes from: https://stackoverflow.com/questions/40416072/reading-a-file-using-a-relative-path-in-a-python-project

ROW_COUNT = 15

national_df = pd.read_csv(csv_files_dir_relative_path / 'National_Interagency_Fire_Occurrence_Sixth_Edition_1992-2020_(Feature_Layer).csv')
with open(csv_files_dir_relative_path / '../docs/heads/NationalDFHead.txt', 'w') as f:
    f.write(national_df.head(ROW_COUNT).to_string())


new_york_df = pd.read_csv(csv_files_dir_relative_path / 'New_York_State_Forest_Ranger_Wildland_Fire_Reporting_Database__Beginning_2008.csv')
with open(csv_files_dir_relative_path / '../docs/heads/NewYorkDFHead.txt', 'w') as f:
    f.write(new_york_df.head(ROW_COUNT).to_string())

oregon_df = pd.read_csv(csv_files_dir_relative_path / 'ODF_Fire_Occurrence_Data_2000-2022.csv')
with open(csv_files_dir_relative_path / '../docs/heads/OregonDFHead.txt', 'w') as f:
    f.write(oregon_df.head(ROW_COUNT).to_string())

california_df = pd.read_csv(csv_files_dir_relative_path / 'California_Historic_Fire_Perimeters_-6236829869961296710.csv')
with open(csv_files_dir_relative_path / '../docs/heads/CaliforniaDFHead.txt', 'w') as f:
    f.write(california_df.head(ROW_COUNT).to_string())