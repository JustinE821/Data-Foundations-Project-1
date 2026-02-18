import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import json
from data_upload.db_constants import JSON_FILE_RELATIVE_PATH
import logging
from data_upload.table_models import log_table
from datetime import datetime
from pathlib import Path

log_path = Path(__file__).parent.resolve() / '..' / '..' / 'logs' / 'conn.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(log_path)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class SQLHandler(logging.Handler):

    def __init__(self, engine):
        super().__init__()
        #log_table.create(engine, checkfirst=True)
        self.__engine = engine
        self.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt='%Y-%m-%d %H:%M:%S'))
        
    def create_base_log(self, record):
        self.format(record)
        timestamp = datetime.strptime(record.asctime.strip(), '%Y-%m-%d %H:%M:%S')
        log = {"timestamp": timestamp, "level": record.levelname, "name_of_table": None, "error_message": None, "num_of_rows_affected": None, "time_elapsed": None, "starting_index": None, "num_of_rows_attempted": None}
        return log

    def create_error_log(self, record):
        log = self.create_base_log(record)
        message_split = record.message.split("|")
        if len(message_split) == 2:
            log["error_message"] = message_split[1][:256]
            log["name_of_table"] = message_split[0]
        elif len(message_split) == 1: # An exception in db_upload.upload_tables() does not have access to 
            # the table name which is provided by error logs in dp_upload.upload_table()
            log["error_message"] = message_split[0]
        else:
            print("This should never happen")
        return log

    def create_warning_log(self, record):
        log = self.create_base_log(record)
        [table_name, error_message, starting_index, num_of_rows_attempted] = record.message.split("|")
        log["name_of_table"] = table_name
        log["error_message"] = error_message[:256]
        log["starting_index"] = int(starting_index)
        log["num_of_rows_attempted"] = int(num_of_rows_attempted)
        log["num_of_rows_affected"] = 0
        return log

    def create_info_log(self, record):
        log = self.create_base_log(record)
        [table_name, starting_index, num_of_rows_attempted, num_of_rows_affected, elapsed_time] = record.message.split("|")
        log["name_of_table"] = table_name
        log["starting_index"] = int(starting_index)
        log["num_of_rows_attempted"] = int(num_of_rows_attempted)
        log["num_of_rows_affected"] = int(num_of_rows_affected)
        log["time_elapsed"] = float(elapsed_time)

        return log

    def emit(self, record):

        level = record.levelname.upper()
        log = {}
        try:
            match level:
                case "INFO":
                    log = self.create_info_log(record)
                case "WARNING":
                    log = self.create_warning_log(record)
                case _:
                    log = self.create_error_log(record)
        except Exception as e:
            logger.error(f"{str(e)}")
            print("Failed to create log: ", str(e))
        
        try:
            with self.__engine.connect() as conn:
                conn.execute(insert(log_table), log)
                conn.commit()
        except Exception as e:
            logger.error(f"{str(e)}")
            print("Failed to write log to database: ", e)
 

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
        logger.error(f"{str(e)}")
        print(f"Database error: {e}")
        raise