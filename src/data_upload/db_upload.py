'''
Docstring for data_upload.df_upload
This file will contain the functions used to add data to the database as well as any other queries
'''
import logging
from sqlalchemy.dialects.postgresql import insert
from data_upload.db_connection import init_conn
from data_upload.table_models import wildfire_table, wildfire_size_table, wildfire_location_table
from pathlib import Path
import time

log_path = Path(__file__).parent.resolve() / '..' / '..' / 'logs' / 'upload.log'
logging.basicConfig(
     filename=log_path, 
     level=logging.DEBUG,
     format="%(asctime)s | %(levelname)s | %(message)s", 
)
logger = logging.getLogger(__name__)

# Row limit will be used to keep a certain amount of records to upload in our demo
# 619,924 total records to upload as of me writing this
ROW_LIMIT = 40_000
BATCH_SIZE = 5_000
START_INDEX = 0

def upload_tables(wildfire_df, wildfire_size_df, wildfire_location_df):
     
     try:
          engine = init_conn()
          upload_table(wildfire_df, engine, wildfire_table, "Wildfire")
          upload_table(wildfire_size_df, engine, wildfire_size_table, "WildfireSize")
          upload_table(wildfire_location_df, engine, wildfire_location_table, "WildfireLocation")
     except Exception as e:
          logger.error(f"Error: {str(e)}")
          print(e)

def upload_table(df, engine, table, table_name):

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
                         start = time.time()
                         result = conn.execute(stmt, entries)
                         end = time.time()
                         log_message = f'{table_name} | {len(batch_df)} | {len(result.all())} | {end - start}s'
                         logger.info(log_message)
                         conn.commit()
     except Exception as e:
          logger.error(f"Error: {str(e)}")
          print(e)
     finally:
          print("Query complete")
