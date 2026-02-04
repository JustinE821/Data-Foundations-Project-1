from data_processing.df_load_driver import get_data_frames

def main():
    # Load data and standardize
    [national_df, new_york_df, oregon_df, california_df] = get_data_frames()

    # Break data into tables

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