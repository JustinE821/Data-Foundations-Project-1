from data_processing.df_load_driver import get_data_frames

def main():
    # Load data
    df = get_data_frames()

    # Write data to db
    print(df)

if __name__ == "__main__":
    main()