'''
Docstring for data_upload.df_upload
This file will contain the functions used to add data to the database as well as any other queries
'''
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text, insert, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from data_upload.db_connection import init_conn
import pandas as pd

def create_wildfire_entries(df):
     
     try:
          engine = init_conn()
          conn = engine.connect()
          # md = MetaData()

          # # md.reflect(bind=conn)
          # wildfire = Table('Wildfire', md, autoload_with=engine)
          # md = wildfire.metadata
          
          

          # df.replace({pd.NaT: None}, inplace=True)
          # #df = df.head(20000).reset_index(drop=True).values.tolist()
          # print(df)
          
          # wildfire_list = list()

          # for index in range(20000):
          #      entry = df.iloc[index].to_dict()
          #      wildfire_list.append(entry)

          # for column in wildfire.columns:

          #      print(column.type)
          # print(md)
          # s = Session()
          
          
          # with Session(engine) as session:
          #      res = session.execute(wildfire.insert(), wildfire_list)
          #      print(res.row)
          #      session.commit()




          # df.to_sql(name='Wildfire', con=conn, if_exists='append', chunksize=5000) # , schema='public'
          # conn.commit()
          start = 0
          end = 1000
          l, h = df.shape
          print(l, " ", h)
          while l > end:
               if len(conn.execute(text(f"SELECT * FROM Wildfire WHERE wildfire_id={df.iloc[start]['wildfire_id']}")).fetchall()) == 0:
                    for index in range(start, end):

                         entry = df.iloc[index].to_dict()
                         if(pd.isna(entry['containment_date'])):
                              entry['containment_date']  = entry['report_date']
                         
                         query = text("INSERT INTO Wildfire(wildfire_id, state, fire_name, cause_id, containment_date, report_date) VALUES(:wildfire_id, :state, :fire_name, :cause_id, :containment_date, :report_date) ON CONFLICT DO NOTHING")           
                         conn.execute(query, entry)
               
               conn.commit()
               start += 1000
               end += 1000
               print(start)

     except Exception as e:
          print("ERROR: ", e)
     finally:
          print("Query complete")
          if conn:
               conn.close()
     return 0




def create_wildfire_location_entries(df):
     #df.replace({pd.NaN: None}, inplace=True)

     try:
          engine = init_conn()
          conn = engine.connect()
          start = 0
          end = 1000
          l, h = df.shape
          while l > end:
               if len(conn.execute(text(f"SELECT * FROM WildfireLocation WHERE wildfire_id={df.iloc[start]['wildfire_id']}")).fetchall()) == 0:
                    for index in range(start, end):

                         entry = df.iloc[index].to_dict()
                         print(index)
                         if entry['longitude'] != None:
                              query = text("INSERT INTO WildfireLocation(wildfire_id, longitude, latitude) VALUES(:wildfire_id, :longitude, :latitude) ON CONFLICT DO NOTHING")           
                              conn.execute(query, entry)
               
               conn.commit()
               start += 1000
               end += 1000
               print(start)

     except Exception as e:
          print("ERROR: ", e)
     finally:
          print("Query complete")
          if conn:
               conn.close()
     return 0

def create_wildfire_size_entries(df):
     #df.replace({pd.NaN: None}, inplace=True)

     try:
          engine = init_conn()
          conn = engine.connect()
          start = 0
          end = 1000
          l, h = df.shape
          while l > end:
               if len(conn.execute(text(f"SELECT * FROM WildfireSize WHERE wildfire_id={df.iloc[start]['wildfire_id']}")).fetchall()) == 0:
                    for index in range(start, end):
                         entry = df.iloc[index].to_dict()
                         query = text("INSERT INTO WildfireSize(wildfire_id, size_class, acreage) VALUES(:wildfire_id, :size_class, :acreage) ON CONFLICT DO NOTHING")           
                         conn.execute(query, entry)
               
               conn.commit()
               start += 1000
               end += 1000
               print(start)

     except Exception as e:
          print("ERROR: ", e)
     finally:
          print("Query complete")
          if conn:
               conn.close()
     return 0

