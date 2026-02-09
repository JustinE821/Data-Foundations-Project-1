'''
Docstring for data_upload.df_upload
This file will contain the functions used to add data to the database as well as any other queries
'''
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from data_upload.db_connection import init_conn
import pandas as pd

def create_wildfire_entries(df):
     
     try:
          engine = init_conn()

          df = df.head(10000)
          for index in range(10000):

               entry = df.iloc[index]
               if(pd.isna(entry['containment_date'])):
                    entry['containment_date']  = entry['report_date']
                    print("foooloo")
               
               sql = f'''INSERT INTO Wildfire(wildfire_id, state, fire_name, cause_id, containment_date, report_date) 
                    VALUES({entry['wildfire_id']}, '{entry['state']}', '{entry['fire_name']}', {entry['cause_id']}, '{entry['containment_date']}', '{entry['report_date']}') ON CONFLICT DO NOTHING'''
               query = text(sql)
               with engine.connect() as conn:
                    conn.execute(query)
                    conn.commit()

     except Exception as e:
          print(e)
     finally:
          print("Query complete")
          # if conn:
          #      conn.close()
     
     
     
     return 0



