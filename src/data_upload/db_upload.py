'''
Docstring for data_upload.df_upload
This file will contain the functions used to add data to the database as well as any other queries
'''
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from data_upload.db_connection import init_conn
from data_upload.table_models import wildfire_table, wildfire_size_table, wildfire_location_table

# Row limit will be used to keep a certain amount of records to upload in our demo
# 619,924 total records to upload as of me writing this
ROW_LIMIT = 40_000
BATCH_SIZE = 5_000
START_INDEX = 0

def upload_tables(wildfire_df, wildfire_size_df, wildfire_location_df):
     
     try:
          engine = init_conn()
          upload_table(wildfire_df, engine, wildfire_table)
          upload_table(wildfire_size_df, engine, wildfire_size_table)
          upload_table(wildfire_location_df, engine, wildfire_location_table)
     except Exception as e:
          print(e)

def upload_table(df, engine, table):

     stmt = insert(table).on_conflict_do_nothing(index_elements=['wildfire_id']).returning(table)
     try:
          with engine.connect() as conn:
               for index in range(START_INDEX, ROW_LIMIT, BATCH_SIZE):
                    batch_df = df.iloc[index : index + BATCH_SIZE]
                    # filter any null longitude and latitude records
                    if table == wildfire_location_table:
                         batch_df = batch_df.dropna(subset=['longitude', 'latitude'])
                    entries = batch_df.to_dict(orient='records')
                    if entries:
                         result = conn.execute(stmt, entries)
                         print(len(result.all()))
                         conn.commit()
     except Exception as e:
          print("ERROR: ", e)
     finally:
          print("Query complete")
