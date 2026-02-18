'''This file is used to query data from the database'''
import logging
from sqlalchemy import text
from data_upload.db_connection import init_conn
from pathlib import Path





log_path = Path(__file__).parent.resolve() / '..' / '..' / 'logs' / 'query.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(log_path)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)




# Gathers all fire entries and groups them by month
def fetch_fire_count_by_month(engine=None):

    if engine == None:
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
        logger.error(f"{str(e)}")
        print(f"ERROR: {e}")

# Returns all fire locations, as well as the severity of said fires
def fetch_fire_coordinates(engine=None):

    if engine == None:
        engine = init_conn()
    
    try:

        sql = text('''SELECT wl.longitude, wl.latitude, ws.acreage, ws.size_class
                        FROM wildfirelocation wl
                        INNER JOIN wildfiresize ws ON wl.wildfire_id=ws.wildfire_id;'''
                   )
        locations = None
        with engine.connect() as conn:
            locations = conn.execute(sql).all()
        return locations

        
        
    except Exception as e:
        logger.error(f"{str(e)}")
        print(f"Error: {e}")
    






# This function gathers the top 4 fire types within a certain acreage range
def fetch_wildfire_count_by_type(fire_size=5, engine=None):

    if engine == None:
        engine = init_conn()

    sql = text('''SELECT wc.cause_text, COUNT(w.cause_id)
                    FROM wildfire w
                    INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
                    INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
                    WHERE ws.acreage > :fire_size
                    GROUP BY wc.cause_text
                    ORDER BY COUNT(w.cause_id) DESC
                    LIMIT 4;'''
               )
    res = None
    try:
        with engine.connect() as conn:
            res = conn.execute(sql, {"fire_size": fire_size})
    except Exception as e:
        logger.error(f"{str(e)}")
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()
    
#Function returns the number of fires that occured within a certain acreage range
def fetch_number_of_fires(fire_size, engine=None):
    if engine == None:
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
        logger.error(f"{str(e)}")
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()
    
#This function returns most common cause of fires in each individual state
def fetch_top_causes(engine=None):
    if engine == None:
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
        logger.error(f"{str(e)}")
        print(f"Error: {str(e)}")
    else:
        return res.fetchall()


def fetch_states_with_highest_acreage_sums(engine=None):
    if engine == None:
        engine = init_conn()

    try:
        sql = text('''SELECT w.state_id, SUM(ws.acreage)
                        FROM wildfire w
                        INNER JOIN wildfiresize ws ON w.wildfire_id=ws.wildfire_id
                        GROUP BY w.state_id
                        ORDER BY SUM(ws.acreage) DESC
                        LIMIT 10;'''
                   )
        res = None
        with engine.connect() as conn:
            res = conn.execute(sql).fetchall()
        return res
    except Exception as e:
        logger.error(f"{str(e)}")
        print(f"ERROR: {e}")


# def fetch_top_fire_cause_by_state(engine=None):

#     if engine == None:
#         engine = init_conn()

#     sql = text('''WITH ranked_causes AS (
#                     SELECT 
#                         w.state_id,
#                         wc.cause_text,
#                         COUNT(w.state_id) AS num_of_occurances,
#                         ROW_NUMBER() OVER (PARTITION BY w.state_id ORDER BY COUNT(w.state_id) DESC) AS row_num
#                     FROM wildfire w
#                     INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
#                     GROUP BY w.state_id, wc.cause_text
#                     )
#                     SELECT state_id, cause_text, num_of_occurances
#                     FROM ranked_causes
#                     WHERE row_num = 1
#                     ORDER BY state_id;'''
#                )
#     res = None
#     try:
#         with engine.connect() as conn:
#             res = conn.execute(sql)
#     except Exception as e:
#         print(f"Error: {str(e)}")
#     else:
#         return res.fetchall()


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




    

    


