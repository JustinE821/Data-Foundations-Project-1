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

          conn = engine.connect()
          df = df.head(10000)
          for index in range(10000):

               entry = df.iloc[index].to_dict()
               if(pd.isna(entry['containment_date'])):
                    entry['containment_date']  = entry['report_date']

               print(index)
               print(entry)
               
               query = text("INSERT INTO Wildfire(wildfire_id, state, fire_name, cause_id, containment_date, report_date) VALUES(:wildfire_id, :state, :fire_name, :cause_id, :containment_date, :report_date) ON CONFLICT DO NOTHING")
               conn.execute(query, entry)
          
          conn.commit()

     except Exception as e:
          print(e)
     finally:
          print("Query complete")
          if conn:
               conn.close()
     return 0



