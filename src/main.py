"""
This file is the entry point for our project. It contains the main method and coordinates 
each step in our data processing pipeline.
1. Load csv files into DataFrames while validating and standardizing columns
2. Combine DataFrames into one and drop duplicates. Create index.
3. Break combined DataFrame into smaller DataFrames corresponding to tables in our database.
4. Write the resulting DataFrames to the database.
"""
from data_processing.df_load_driver import get_dataframes
from data_processing.combine_dataframes import combine_dataframes
from data_processing.split_dataframes import split_dataframes
from data_upload.db_upload import create_wildfire_entries, create_wildfire_location_entries, create_wildfire_size_entries

def show_original_dataframes(national_df, new_york_df, oregon_df, california_df):
    print('-----------------------')
    print("National DF")
    print(national_df.head())
    print('-----------------------')
    print("New York DF")
    print(new_york_df.head())
    print('-----------------------')
    print("Oregon DF")
    print(oregon_df.head())
    print('-----------------------')
    print("California DF")
    print(california_df.head())
    print('-----------------------')

def show_table_dataframes(wildfire_df, wildfire_size_df, wildfire_location_df):
    print("--------------------")
    print(wildfire_df.head())
    print("--------------------")
    print(wildfire_size_df.head())
    print("--------------------")
    print(wildfire_location_df.head())
    print("--------------------")

def main():
    # Load data and standardize
    df_list = get_dataframes()

    # Combine DataFrames and drop duplicates
    combined_df = combine_dataframes(df_list)
    #print(combined_df.shape)
    
    # Break DataFrame into tables
    [wildfire_df, wildfire_size_df, wildfire_location_df] = split_dataframes(combined_df=combined_df)

    # Write data to db
    create_wildfire_entries(wildfire_df)

    # Write data to location table
    #create_wildfire_location_entries(wildfire_location_df)

    # Write data to the size table
    #create_wildfire_size_entries(wildfire_size_df)

    #show_original_dataframes(*df_list)
    #show_table_dataframes(wildfire_df, wildfire_size_df, wildfire_location_df)

if __name__ == "__main__":
    main()