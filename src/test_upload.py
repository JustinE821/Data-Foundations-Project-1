import pytest
from mockito import mock, verify, captor, when, args
from data_upload.db_connection import SQLHandler
import logging
from data_upload.db_upload import upload_table
from data_upload.table_models import wildfire_table, wildfire_location_table
import datetime
import pandas as pd
from data_upload.db_query import fetch_fire_count_by_month, fetch_fire_coordinates, fetch_number_of_fires, fetch_top_causes, fetch_wildfire_count_by_type, fetch_states_with_highest_acreage_sums
from unittest.mock import MagicMock
from data_upload.db_connection import init_conn

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


def test_fetch_fire_coordinates_success():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    
    #Setting mock db to contain some values
    mock_result = [(-78.234565, 40.345643, 3.50, 'B'), (-78.234565, 42.345643, 3.50, 'B')]
    mock_conn.execute.return_value.all.return_value = mock_result

    # When connect() is called on the engine, return the mock connection
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    

    # Call the function with mock engine
    result = fetch_fire_coordinates(mock_engine)

    # Verify the SQL ran in the mocked function is correct
    args, kwargs = mock_conn.execute.call_args
    assert 'SELECT wl.longitude, wl.latitude, ws.acreage, ws.size_class' in str(args[0])  # Check SQL string
    assert mock_result == result

def test_fetch_fire_count_by_month_success():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    
    #Setting mock db to contain some values
    mock_result = [(1.0, 30447), (2.0, 38916)]
    mock_conn.execute.return_value.fetchall.return_value = mock_result
    
    # When connect() is called on the engine, return the mock connection
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    

    # Call the function with mock engine
    result = fetch_fire_count_by_month(mock_engine)



    # Verify the SQL ran in the mocked function is correct
    args, kwargs = mock_conn.execute.call_args
    assert 'SELECT DATE_PART' in str(args[0])  # Check SQL string
    assert mock_result == result


def test_fetch_wildfire_content_by_type_success():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    
    #Setting mock db to contain some values
    mock_result = [("Open Burning", 19173), ("Unknown", 18345)]
    mock_conn.execute.return_value.fetchall.return_value = mock_result
    
    # When connect() is called on the engine, return the mock connection
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    

    # Call the function with mock engine
    result = fetch_wildfire_count_by_type(10, mock_engine)



    # Verify the SQL ran in the mocked function is correct
    args, kwargs = mock_conn.execute.call_args
    assert 'SELECT wc.cause_text, COUNT(w.cause_id)' in str(args[0])  # Check SQL string
    assert mock_result == result

def test_fetch_number_of_fires_success():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    
    #Setting mock db to contain some values
    mock_result = [(75047)]
    mock_conn.execute.return_value.fetchall.return_value = mock_result
    
    # When connect() is called on the engine, return the mock connection
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    # Call the function with mock engine
    result = fetch_number_of_fires(10 ,mock_engine)

    # Verify the SQL ran in the mocked function is correct
    args, kwargs = mock_conn.execute.call_args
    assert 'SELECT COUNT(w.wildfire_id)' in str(args[0])  # Check SQL string
    assert mock_result == result

def test_fetch_top_causes_success():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    
    #Setting mock db to contain some values
    mock_result = [("Arson", 6.0), ("Equipment/Vehicle", 1.0), ("Juvenile", 1.0), ("Natural", 12.0), ("Open Burning", 32.0)]
    #If any entry is removed from mock result after it is copied, the assert later in the code will fail
    mock_conn.execute.return_value.fetchall.return_value = mock_result.copy()
    #mock_result.remove(("Arson", 6.0))


    # When connect() is called on the engine, return the mock connection
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    

    # Call the function with mock engine
    result = fetch_top_causes(mock_engine)

    # Verify the SQL ran in the mocked function is correct
    args, kwargs = mock_conn.execute.call_args
    assert 'SELECT cause_text, COUNT(cause_text)' in str(args[0])  # Check SQL string
    assert mock_result == result

def test_fetch_states_with_highest_acreage_sums_success():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    
    #Setting mock db to contain some values
    mock_result = [("VA", 1234567890.0), ("AK", 123456789.0), ("CA", 12345678.0), ("OR", 1234567.0), ("NV", 123456.0), ("WA", 12345.0), ("TX", 1234.0), ("MT", 123.0), ("AZ", 12.0), ("ID", 12.0)]
    mock_conn.execute.return_value.fetchall.return_value = mock_result
    
    # When connect() is called on the engine, return the mock connection
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    

    # Call the function with mock engine
    result = fetch_states_with_highest_acreage_sums(mock_engine)



    # Verify the SQL ran in the mocked function is correct
    args, kwargs = mock_conn.execute.call_args
    assert 'SELECT w.state_id, SUM(ws.acreage)' in str(args[0])  # Check SQL string
    assert mock_result == result