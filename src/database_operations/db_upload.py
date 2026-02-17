'''
Docstring for database_operations.df_upload
This file will contain the functions used to add data to the database as well as any other queries
'''
import logging
from sqlalchemy.dialects.postgresql import insert
from database_operations.db_connection import init_conn, SQLHandler
from database_operations.table_models import wildfire_table, wildfire_size_table, wildfire_location_table
from pathlib import Path
import time

log_path = Path(__file__).parent.resolve() / '..' / '..' / 'logs' / 'upload.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(log_path)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# There are 588,733 records in wildfire_df
ROW_LIMIT = 590_000
BATCH_SIZE = 5_000
START_INDEX = 0

def upload_tables(wildfire_df, wildfire_size_df, wildfire_location_df):
     
     try:
          engine = init_conn()
          sql_handler = SQLHandler(engine)
          logger.addHandler(sql_handler)
          upload_table(wildfire_df, engine, wildfire_table, "Wildfire", START_INDEX, ROW_LIMIT, BATCH_SIZE)
          upload_table(wildfire_size_df, engine, wildfire_size_table, "WildfireSize", START_INDEX, ROW_LIMIT, BATCH_SIZE)
          upload_table(wildfire_location_df, engine, wildfire_location_table, "WildfireLocation", START_INDEX, ROW_LIMIT, BATCH_SIZE)
     except Exception as e:
          logger.error(f"Error: {str(e)}")
          print(e)

def upload_table(df, engine, table, table_name, start_index, row_limit, batch_size):

     stmt = insert(table).on_conflict_do_nothing(index_elements=['wildfire_id']).returning(table)
     try:
          with engine.connect() as conn:
               for index in range(start_index, row_limit, batch_size):
                    try:
                         batch_df = df.iloc[index : index + batch_size]
                         # filter any null longitude and latitude records
                         if table_name == 'wildfirelocation':
                              batch_df = batch_df.dropna(subset=['longitude', 'latitude'])
                         entries = batch_df.to_dict(orient='records')
                         if entries:
                              start = time.time()
                              result = conn.execute(stmt, entries)
                              end = time.time()
                              log_message = f'{table_name}|{index}|{len(batch_df)}|{len(result.all())}|{round(end - start, 4)}'
                              conn.commit()
                              logger.info(log_message)
                    except Exception as e:
                         logger.warning(f"{table_name}|{str(e)}|{index}|{BATCH_SIZE}")
     except Exception as e:
          logger.error(f"{table_name}|{str(e)}")
          print(e)
     finally:
          print("Query complete")
