'''This file is used to query data from the database'''
import logging
from sqlalchemy import text, select
from data_upload.db_connection import init_conn
from data_upload.table_models import wildfire_table, wildfire_size_table, wildfire_location_table
from pathlib import Path
import time





def fetch_fire_count_by_month():

    engine = init_conn()

    try:
        sql = text('''SELECT DATE_PART('month', report_date) AS fires_by_month, COUNT(wildfire_id)
                        FROM wildfire
                        GROUP BY DATE_PART('month', report_date)
                        ORDER BY fires_by_month ASC;'''
                   )

        res = None
        with engine.connect() as conn:
            res = conn.execute(sql).fetchall()

        return res

    except Exception as e:
        print(f"ERROR: {e}")


def fetch_fire_coordinates():

    engine = init_conn()
    
    try:

        stmt = select(wildfire_location_table)
        sql = text('''SELECT wl.longitude, wl.latitude, ws.acreage, ws.size_class
                        FROM wildfirelocation wl
                        INNER JOIN wildfiresize ws ON wl.wildfire_id=ws.wildfire_id;'''
                   )
        locations = None
        with engine.connect() as conn:
            #locations = conn.execute(stmt).all()
            locations = conn.execute(sql).all()
        return locations

        
        
    except Exception as e:
        print(f"Error: {e}")
    





def fetch_top_fire_cause_by_state():

    engine = init_conn()
    sql = text('''WITH ranked_causes AS (
                    SELECT 
                        w.state_id,
                        wc.cause_text,
                        COUNT(w.state_id) AS num_of_occurances,
                        ROW_NUMBER() OVER (PARTITION BY w.state_id ORDER BY COUNT(w.state_id) DESC) AS row_num
                    FROM wildfire w
                    INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
                    GROUP BY w.state_id, wc.cause_text
                    )
                    SELECT state_id, cause_text, num_of_occurances
                    FROM ranked_causes
                    WHERE row_num = 1
                    ORDER BY state_id;'''
               )
    res = None
    try:
        with engine.connect() as conn:
            res = conn.execute(sql)
    except Exception as e:
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()
    
def fetch_wildfire_count_by_type(fire_size):

    engine = init_conn()
    sql = text('''SELECT wc.cause_text, COUNT(w.cause_id)
                    FROM wildfire w
                    INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
                    INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
                    WHERE ws.acreage > :fire_size
                    GROUP BY wc.cause_text
                    ORDER BY COUNT(w.cause_id) DESC
                    LIMIT 5;'''
               )
    res = None
    try:
        with engine.connect() as conn:
            res = conn.execute(sql, {"fire_size": fire_size})
    except Exception as e:
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()
    
def fetch_number_of_fires(fire_size):
    engine = init_conn()
    sql = text('''SELECT COUNT(w.wildfire_id) 
                    FROM wildfire w
                    INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
                    WHERE ws.acreage > :fire_size;'''
               )
    res = None
    try:
        with engine.connect() as conn:
            res = conn.execute(sql, {"fire_size": fire_size})
    except Exception as e:
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()
    

def fetch_top_causes():
    engine = init_conn()
    sql = text('''WITH ranked_causes AS (
                    SELECT 
                        w.state_id,
                        wc.cause_text,
                        COUNT(w.state_id) AS num_of_occurances,
                        ROW_NUMBER() OVER (PARTITION BY w.state_id ORDER BY COUNT(w.state_id) DESC) AS row_num
                    FROM wildfire w
                    INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
                    INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
                    WHERE w.cause_id != 11
                    GROUP BY w.state_id, wc.cause_text
                    )
                    SELECT cause_text, COUNT(cause_text)
                    FROM ranked_causes
                    WHERE row_num = 1
                    GROUP BY cause_text;'''
                   )
    try:
        with engine.connect() as conn:
            res = conn.execute(sql)
    except Exception as e:
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()




# def fetch_wildfires_count_by_state():

#     engine = init_conn()
#     sql = text('''SELECT state_id, COUNT(state_id) 
#                 FROM wildfire
#                 GROUP BY state_id
#                 ORDER BY COUNT(state_id) DESC;'''
#                )
#     res = None
#     with engine.connect() as conn:
#         res = conn.execute(sql)



#     return res.fetchall()

# def fetch_acres_burned_by_state():
#     engine = init_conn()


#     try:
#         sql = text('''SELECT w.state_id, SUM(ws.acreage)
#                         FROM wildfire w
#                         INNER JOIN wildfiresize ws ON w.wildfire_id=ws.wildfire_id
#                         GROUP BY w.state_id
#                         ORDER BY SUM(ws.acreage) DESC;'''
#                    )
#         res = None
#         with engine.connect() as conn:
#             res = conn.execute(sql).fetchall()
#         return res
#     except Exception as e:
#         print(f"ERROR: {e}")


# def fetch_wildfires_cause_by_state(state):

#     engine = init_conn()
#     state_mapping = {"state": state}
#     sql = text('''SELECT w.state_id, wc.cause_text, COUNT(w.state_id) 
#                 FROM wildfire w
#                 INNER JOIN wildfirecause wc ON w.cause_id=wc.cause_id
#                 WHERE w.state_id = :state 
#                 GROUP BY w.state_id, wc.cause_text
#                 ORDER BY w.state_id, COUNT(w.state_id) DESC;'''
#                )
    
#     with engine.connect() as conn:
#         res = conn.execute(sql, state_mapping)
#         print(res.fetchall())

# def fetch_fire_count_by_state():
    
#     engine = init_conn()
#     sql = text('''SELECT state_id, COUNT(state_id) 
#                     FROM wildfire
#                     GROUP BY state_id
#                     ORDER BY COUNT(state_id) DESC;'''
#                )
#     try:
#         with engine.connect() as conn:
#             res = conn.execute(sql)
#     except Exception as e:
#         print(f"Error: {str(e)}")
#     else:
#         return res.fetchall()   




    

    


