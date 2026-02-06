"""
This file is the entry point for our project. It contains the main method and coordinates 
each step in our data processing pipeline.
1. Load csv files into DataFrames while validating and standardizing columns
2. Combine DataFrames into one and drop duplicates. Create index.
3. Break combined DataFrame into smaller DataFrames corresponding to tables in our database.
4. Write the resulting DataFrames to the database.
"""
from data_processing.df_load_driver import get_data_frames

def main():
    # Load data and standardize
    [national_df, new_york_df, oregon_df, california_df] = get_data_frames()

    # Combine DataFrames and drop duplicates

    # Break DataFrame into tables

    # Write data to db
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

if __name__ == "__main__":
    main()
