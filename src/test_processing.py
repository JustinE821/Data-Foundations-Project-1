'''This file will be used to test the functions used across the project'''
import pytest
import pandas as pd
import numpy
import datetime
from data_processing.standardize_columns import create_size_class, limit_date_range, compute_date_cols, standardize_column_names, convert_datetime_to_date
from data_processing.converter_functions import cause_converter
from data_processing.combine_dataframes import combine_dataframes, log_duplicate_key, log_null_key
from data_processing.split_dataframes import split_dataframes
from data_processing.constants import NATIONAL_COLS, NEW_YORK_COLS, OREGON_COLS, CALIFORNIA_COLS, NATIONAL_TO_STANDARD_COLUMN_MAPPING, NEW_YORK_TO_STANDARD_COLUMN_MAPPING, OREGON_TO_STANDARD_COLUMN_MAPPING, CALIFORNIA_TO_STANDARD_COLUMN_MAPPING, STANDARD_COLUMN_NAMES, WILDFIRE_COLUMN_NAMES, WILDFIRE_LOCATION_COLUMN_NAMES, WILDFIRE_SIZE_COLUMN_NAMES
from mockito import mock, verify, captor



def test_cause_converter_success():
    cause = 'Lightning'

    result = cause_converter(cause)

    assert result == 1

def test_cause_converter_unexpected_input():
    cause = 'aldskfadj'

    result = cause_converter(cause)

    assert result == 11

def test_create_size_class_success():
    acreage_col_name = 'Acreage'
    data = {acreage_col_name: [4, 4000]}
    df = pd.DataFrame(data=data)
    expected_result = pd.Series(['B', 'F'])

    result = create_size_class(df, acreage_col_name)


    assert result.equals(expected_result)


def test_limit_date_range_success():
    data = {'report_date': [datetime.date(2010, 12, 1), datetime.date(2026, 9, 1), datetime.date(2001, 1, 1)]}
    df = pd.DataFrame(data=data)
    expected_result = pd.DataFrame(data={'report_date': [datetime.date(2010, 12, 1)]})

    result = limit_date_range(df)


    assert result.equals(expected_result)

def test_compute_date_cols_sucess():
    data_columns = NATIONAL_COLS
    expected_result = ['DISCOVERY_DATE', 'CONT_DATE']

    result = compute_date_cols(data_columns)


    assert result == expected_result

def test_standardize_column_names_success():
    national_df = pd.DataFrame(columns=NATIONAL_COLS)
    oregon_df = pd.DataFrame(columns=OREGON_COLS)
    new_york_df = pd.DataFrame(columns=NEW_YORK_COLS)
    california_df = pd.DataFrame(columns=CALIFORNIA_COLS)

    expected_nat_df = national_df.rename(columns=NATIONAL_TO_STANDARD_COLUMN_MAPPING)
    expected_ny_df = new_york_df.rename(columns=NEW_YORK_TO_STANDARD_COLUMN_MAPPING)
    expected_or_df = oregon_df.rename(columns=OREGON_TO_STANDARD_COLUMN_MAPPING)
    expected_cali_df = california_df.rename(columns=CALIFORNIA_TO_STANDARD_COLUMN_MAPPING)

    result = standardize_column_names(national_df, new_york_df, oregon_df, california_df)


    assert result[0].equals(expected_nat_df) and result[1].equals(expected_ny_df) and result[2].equals(expected_or_df) and result[3].equals(expected_cali_df)

def test_combine_dataframes_success_multiple_duplicates():
    data1 = {'state_id': ['NC', 'OR'], 'report_date': [datetime.date(2010, 1, 1), datetime.date(2010, 1, 1)], 'fire_name': ['test fire', 'another fire']}
    df1 = pd.DataFrame(data=data1)

    data2 = {'state_id': ['NC', 'OR', 'NY'], 'report_date': [datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2012, 5, 5)], 'fire_name': ['test fire', 'another fire', 'One more fire']}
    df2 = pd.DataFrame(data=data2)

    expected_data = { 'wildfire_id': [0, 1, 2], 'state_id': ['NC', 'OR', 'NY'], 'report_date': [datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2012, 5, 5)], 'fire_name': ['test fire', 'another fire', 'One more fire']}
    expected_result = pd.DataFrame(data=expected_data)
    
    result = combine_dataframes([df1, df2])
    print("\n\n", result)
    print("\n\n", expected_result)
    assert result.shape == (3, 4)
    pd.testing.assert_frame_equal(expected_result, result)

def test_combine_dataframes_success_multiple_duplicates_and_unique_both_sides():
    data1 = {'state_id': ['NC', 'OR'], 'report_date': [datetime.date(2010, 1, 1), datetime.date(2010, 2, 1)], 'fire_name': ['test fire', 'another fire2']}
    df1 = pd.DataFrame(data=data1)

    data2 = {'state_id': ['NC', 'OR', 'NY'], 'report_date': [datetime.date(2010, 1, 1), datetime.date(2010, 1, 1), datetime.date(2012, 5, 5)], 'fire_name': ['test fire', 'another fire', 'One more fire']}
    df2 = pd.DataFrame(data=data2)

    expected_data = { 'wildfire_id': [0, 1, 2, 3], 'state_id': ['NC', 'OR', 'OR', 'NY'], 'report_date': [datetime.date(2010, 1, 1), datetime.date(2010, 2, 1), datetime.date(2010, 1, 1), datetime.date(2012, 5, 5)], 'fire_name': ['test fire', 'another fire2', 'another fire', 'One more fire']}
    expected_result = pd.DataFrame(data=expected_data)
    
    result = combine_dataframes([df1, df2])
    print("\n\n", result)
    print("\n\n", expected_result)
    assert result.shape == (4, 4)
    pd.testing.assert_frame_equal(expected_result, result)

def test_log_null_key_drop_rows_any_column():
    logger = mock()
    test_data = { 'fire_name': [1, 2, 3, None, 5], 'state_id': ['VA', 'NY', None, 'AK', 'CA'], 'report_date': [datetime.date(2020, 1, 1), None, datetime.date(2022, 2, 27), None, datetime.date(1999, 8, 13)]}
    test_df = pd.DataFrame(data=test_data)
    result_data = { 'fire_name': [2, 3, None], 'state_id': ['NY', None, 'AK'], 'report_date': [None, datetime.date(2022, 2, 27), None]}
    result_df = pd.DataFrame(data=result_data, index=[1, 2, 3])
    expected_log = f"\n{result_df.to_string()}"

    log_null_key(test_df, logger)

    verify(logger).info(expected_log)

def test_log_null_key_drop_rows_state_id_only():
    logger = mock()
    arg = captor()
    test_data = { 'fire_name': ["Fire1", "Fire2", "Fire3", "Fire4", "Fire5"], 'state_id': ['VA', 'NY', None, 'AK', 'CA'], 'report_date': [datetime.date(2020, 1, 1), datetime.date(2021, 2, 2), datetime.date(2022, 2, 27), datetime.date(2023, 3, 3), datetime.date(1999, 8, 13)]}
    test_df = pd.DataFrame(data=test_data)

    log_null_key(test_df, logger)

    verify(logger).info(arg)
    assert "Fire3" in arg.value

def test_log_null_key_drop_rows_fire_name_only():
    logger = mock()
    arg = captor()
    test_data = { 'fire_name': ["Fire1", "Fire2", None, "Fire4", "Fire5"], 'state_id': ['VA', 'NY', 'TX', 'AK', 'CA'], 'report_date': [datetime.date(2020, 1, 1), datetime.date(2021, 2, 2), datetime.date(2022, 2, 27), datetime.date(2023, 3, 3), datetime.date(1999, 8, 13)]}
    test_df = pd.DataFrame(data=test_data)

    log_null_key(test_df, logger)

    verify(logger).info(arg)
    assert 'None' in arg.value or 'NaN' in arg.value
    assert 'TX' in arg.value

def test_log_null_key_drop_rows_report_date_only():
    logger = mock()
    test_data = { 'fire_name': [1, 2, 3, 4, 5], 'state_id': ['VA', 'NY', 'TX', 'AK', 'CA'], 'report_date': [datetime.date(2020, 1, 1), datetime.date(2021, 2, 2), None, datetime.date(2023, 3, 3), datetime.date(1999, 8, 13)]}
    test_df = pd.DataFrame(data=test_data)
    result_data = { 'fire_name': [3], 'state_id': ['TX'], 'report_date': [None]}
    result_df = pd.DataFrame(data=result_data, index=[2])
    expected_log = f"\n{result_df.to_string()}"

    log_null_key(test_df, logger)

    verify(logger).info(expected_log)

def test_log_null_key_no_dropped_rows():
    logger = mock()
    test_data = { 'fire_name': [1, 2, 3, 4, 5], 'state_id': ['VA', 'NY', 'TX', 'AK', 'CA'], 'report_date': [datetime.date(2020, 1, 1), datetime.date(2021, 2, 2), datetime.date(2022, 2, 27), datetime.date(2023, 3, 3), datetime.date(1999, 8, 13)]}
    test_df = pd.DataFrame(data=test_data)
    result_df = pd.DataFrame(data={ 'fire_name': [], 'state_id': [], 'report_date': []})
    expected_log = f"\n{result_df.to_string()}"

    log_null_key(test_df, logger)

    verify(logger).info(expected_log)

def test_split_dataframes_success():
    data = {'wildfire_id': [1], 'fire_name': ['test'], 'report_date': [datetime.date(2010, 1, 1)], 'cause': [1], 'containment_date': [datetime.date(2010, 1, 1)], 'acreage': [4], 'size_class': ['A'], 'latitude': [35.35534], 'longitude': [35.354], 'state_id': ['NC']}
    df = pd.DataFrame(data=data)

    expected_df1 = pd.DataFrame(data= {'wildfire_id': [1], 'state_id': ['NC'], 'fire_name': ['test'], 'cause_id': [1], 'containment_date': [datetime.date(2010, 1, 1)], 'report_date': [datetime.date(2010, 1, 1)]})
    expected_df2 = pd.DataFrame(data= {'wildfire_id': [1], 'size_class': ['A'], 'acreage': [4]})
    expected_df3 = pd.DataFrame(data= {'wildfire_id': [1], 'longitude': [35.354], 'latitude': [35.35534]})
    result = split_dataframes(df)
    assert result[0].equals(expected_df1) and result[1].equals(expected_df2) and result[2].equals(expected_df3)
