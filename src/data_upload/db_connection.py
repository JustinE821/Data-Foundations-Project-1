import psycopg2
import boto3
import json
from data_upload.db_constants import JSON_FILE_RELATIVE_PATH




def init_conn():

    secrets = ''
    with open(JSON_FILE_RELATIVE_PATH, 'r') as file:
        secrets = json.load(file)

    conn = None
    try:
        conn = psycopg2.connect(
            host=secrets['host'],
            database=secrets['database'],
            user=secrets['user'],
            password=secrets['password']
        )
        cur = conn.cursor()
        cur.execute('SELECT * FROM WildfireSizeClass')
        print(cur.fetchall())
        cur.close()
    except Exception as e:
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()