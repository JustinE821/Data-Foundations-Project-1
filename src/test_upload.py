import pytest
from mockito import mock, verify, captor, when, args
from data_upload.db_connection import SQLHandler
import logging

@pytest.fixture
def sql_handler():
    arg = captor()
    arg2 = captor()
    
    conn = mock()
    when(conn).__enter__().thenReturn(conn)
    when(conn).__exit__().thenReturn(None)

    engine = mock()
    when(engine).connect().thenReturn(conn)
    
    return [SQLHandler(engine), conn, arg, arg2]

def test_SQLHandler_emit_error(sql_handler):
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