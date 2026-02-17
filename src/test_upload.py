import pytest
from mockito import mock, verify, captor, when, args
from data_upload.db_connection import SQLHandler
import logging
from data_upload.db_upload import upload_table
from data_upload.table_models import wildfire_table, wildfire_location_table
import datetime
import pandas as pd

@pytest.fixture
def engine_connection():
    conn = mock()
    when(conn).__enter__().thenReturn(conn)
    when(conn).__exit__().thenReturn(None)

    engine = mock()
    when(engine).connect().thenReturn(conn)
    return [engine, conn]

def test_upload_table(engine_connection):
    arg = captor()
    arg2 = captor()
    [engine, conn] = engine_connection
    test_data = {
        'wildfire_id': [1, 2, 3], 
        'state_id': ['VA', 'VA', 'NC'], 
        'fire_name': ['Fire1', 'Fire2', 'Fire3'], 
        'report_date': [datetime.date(2010, 4, 29), datetime.date(2011, 6, 28), datetime.date(2013, 4, 5)], 
        'containment_date': [datetime.date(2010, 4, 29), datetime.date(2011, 6, 29), datetime.date(2013, 4, 5)],
        'cause_id': [1, 1, 11]
    }
    test_df = pd.DataFrame(data=test_data)

    upload_table(test_df, engine, wildfire_table, "wildfire", 0, 3, 1)

    verify(conn, 3).execute(arg, arg2)

def test_upload_table_wildfire_location_drops_null(engine_connection):
    arg = captor()
    arg2 = captor()
    [engine, conn] = engine_connection
    test_data = {
        'wildfire_id': [1, 2, 3], 
        'longitude': [1, None, 11],
        'latitude': [1, 2, 1]
    }
    test_df = pd.DataFrame(data=test_data)

    upload_table(test_df, engine, wildfire_location_table, "wildfirelocation", 0, 3, 1)

    verify(conn, 2).execute(arg, arg2)

@pytest.fixture
def sql_handler(engine_connection):
    [conn, engine] = engine_connection
    arg = captor()
    arg2 = captor()
    
    conn = mock()
    when(conn).__enter__().thenReturn(conn)
    when(conn).__exit__().thenReturn(None)

    engine = mock()
    when(engine).connect().thenReturn(conn)

    return [SQLHandler(engine), conn, arg, arg2]

def test_SQLHandler_emit_error_with_table_name(sql_handler):
    [handler, conn, arg, arg2] = sql_handler
    
    record = logging.LogRecord(
        name='test_log_record',
        level=logging.ERROR,
        pathname='test.py',
        lineno=1,
        msg='wildfire|test error message',
        args=(),
        exc_info=None
    )

    handler.emit(record)

    verify(conn).execute(arg, arg2)

    assert arg2.value['level'] == "ERROR"
    assert arg2.value['name_of_table'] == 'wildfire'
    assert arg2.value['error_message'] == 'test error message'

def test_SQLHandler_emit_error_no_table_name(sql_handler):
    [handler, conn, arg, arg2] = sql_handler
    
    record = logging.LogRecord(
        name='test_log_record',
        level=logging.ERROR,
        pathname='test.py',
        lineno=1,
        msg='test error message',
        args=(),
        exc_info=None
    )

    handler.emit(record)

    verify(conn).execute(arg, arg2)

    assert arg2.value['level'] == "ERROR"
    assert arg2.value['name_of_table'] == None
    assert arg2.value['error_message'] == 'test error message'

def test_SQLHandler_emit_warning(sql_handler):
    [handler, conn, arg, arg2] = sql_handler
    
    record = logging.LogRecord(
        name='test_log_record',
        level=logging.WARNING,
        pathname='test.py',
        lineno=1,
        msg='wildfire|test error message|15|5',
        args=(),
        exc_info=None
    )

    handler.emit(record)

    verify(conn).execute(arg, arg2)

    assert arg2.value['level'] == "WARNING"
    assert arg2.value['name_of_table'] == 'wildfire'
    assert arg2.value['error_message'] == 'test error message'
    assert arg2.value['starting_index'] == 15
    assert arg2.value['num_of_rows_attempted'] == 5

def test_SQLHandler_emit_info(sql_handler):
    [handler, conn, arg, arg2] = sql_handler
    
    record = logging.LogRecord(
        name='test_log_record',
        level=logging.INFO,
        pathname='test.py',
        lineno=1,
        msg='wildfire|10|5|3|0.2343',
        args=(),
        exc_info=None
    )

    handler.emit(record)

    verify(conn).execute(arg, arg2)

    assert arg2.value['level'] == "INFO"
    assert arg2.value['name_of_table'] == 'wildfire'
    assert arg2.value['num_of_rows_attempted'] == 5
    assert arg2.value['starting_index'] == 10
    assert arg2.value['num_of_rows_affected'] == 3
    assert arg2.value['time_elapsed'] == 0.2343