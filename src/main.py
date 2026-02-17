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
from data_upload.db_upload import upload_tables
from data_upload.db_query import fetch_wildfire_count_by_type, fetch_number_of_fires, fetch_top_causes, fetch_fire_coordinates, fetch_fire_count_by_month
from data_exploration.data_plotting import graph_top_causes, graph_state_top_causes, plot_usa_heatmap, graph_fire_count_by_month

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

def show_graphs():

    #One of the key visualization calls
    graph_state_top_causes(fetch_top_causes())

    top_causes = [fetch_wildfire_count_by_type(0), fetch_wildfire_count_by_type(5), fetch_wildfire_count_by_type(100), fetch_wildfire_count_by_type(1000)]
    total_fires = [fetch_number_of_fires(0), fetch_number_of_fires(5), fetch_number_of_fires(100), fetch_number_of_fires(1000)]
    if top_causes != None:
        graph_top_causes(top_causes, total_fires)

    # Used to gather data from the db to then be used to generate a heat map
    fire_location_list = fetch_fire_coordinates()

    if fire_location_list != None:
        plot_usa_heatmap(fire_location_list)

    #This set of functions will give us a graph which we will be able to derive a correlation from
    #The warmer months which contain dead vegetation are more fire prone months
    fires_by_month = fetch_fire_count_by_month()
    if fires_by_month != None:
        graph_fire_count_by_month(fires_by_month)

    



    



def main():
    # Load data and standardize
    #df_list = get_dataframes()

    # Combine DataFrames and drop duplicates
    #combined_df = combine_dataframes(df_list)
    #print(combined_df.shape)

    # Break DataFrame into tables
    #[wildfire_df, wildfire_size_df, wildfire_location_df] = split_dataframes(combined_df=combined_df)

    # Write data to db
    #upload_tables(wildfire_df=wildfire_df, wildfire_size_df=wildfire_size_df, wildfire_location_df=wildfire_location_df)
    
    #show_original_dataframes(*df_list)
    #show_table_dataframes(wildfire_df, wildfire_size_df, wildfire_location_df)

    show_graphs()

if __name__ == "__main__":
    main()