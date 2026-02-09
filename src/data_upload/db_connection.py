import psycopg2
from sqlalchemy import create_engine
import json
from data_upload.db_constants import JSON_FILE_RELATIVE_PATH




def init_conn():

    secrets = ''
    with open(JSON_FILE_RELATIVE_PATH, 'r') as file:
        secrets = json.load(file)

    conn = None
    #engine = create_engine(f"postgresql+psycopg2://{secrets['user']}:{secrets['password']}@{secrets['host']}:5432/{secrets['database']}")


    try:
        #conn = engine.connect()
        engine = create_engine(f"postgresql+psycopg2://{secrets['user']}:{secrets['password']}@{secrets['host']}:5432/{secrets['database']}")

        # conn = psycopg2.connect(
        #     host=secrets['host'],
        #     database=secrets['database'],
        #     user=secrets['user'],
        #     password=secrets['password']
        # )
        print("Success")
        return engine

        #return engine
    except Exception as e:
        print(f"Database error: {e}")
        raise