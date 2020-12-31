import pytest
import pandas as pd

from splink_data_standardisation.remove_anomalies import null_out_values,null_out_values_array
from pyspark.sql import Row


def test_null_out_vals_0(spark):

    data_list = [
        {"id": 1, "mycol": "A"},
        {"id": 2, "mycol": "B"},
        {"id": 3, "mycol": "B"},
        {"id": 4, "mycol": "C"},
        {"id": 5, "mycol": "C"},
    ]

    garbagevals = []

    df = spark.createDataFrame(Row(**x) for x in data_list)

    df = null_out_values(df, "mycol", garbagevals)

    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "mycol": "A"},
        {"id": 2, "mycol": "B"},
        {"id": 3, "mycol": "B"},
        {"id": 4, "mycol": "C"},
        {"id": 5, "mycol": "C"},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)


def test_null_out_vals_1(spark):

    data_list = [
        {"id": 1, "mycol": "A"},
        {"id": 2, "mycol": "B"},
        {"id": 3, "mycol": "B"},
        {"id": 4, "mycol": "C"},
        {"id": 5, "mycol": "C"},
    ]

    garbagevals = ["C"]

    df = spark.createDataFrame(Row(**x) for x in data_list)

    df = null_out_values(df, "mycol", garbagevals)

    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "mycol": "A"},
        {"id": 2, "mycol": "B"},
        {"id": 3, "mycol": "B"},
        {"id": 4, "mycol": None},
        {"id": 5, "mycol": None},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)

    

def test_null_out_array_1(spark):

    data_list = [
        {"id": 1, "mycol": ["A","D"]},
        {"id": 2, "mycol": ["B"]},
        {"id": 3, "mycol": ["B"]},
        {"id": 4, "mycol": ["C"]},
        {"id": 5, "mycol": ["C"]},
    ]

    garbagevals = ["C","D"]

    df = spark.createDataFrame(Row(**x) for x in data_list)

    df = null_out_values_array(df, "mycol", garbagevals)

    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "mycol": ["A",None]},
        {"id": 2, "mycol": ["B"]},
        {"id": 3, "mycol": ["B"]},
        {"id": 4, "mycol": [None]},
        {"id": 5, "mycol": [None]},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)

    
def test_null_out_array_fallback(spark):

    data_list = [
        {"id": 1, "mycol": "A"},
        {"id": 2, "mycol": "B"},
        {"id": 3, "mycol": "B"},
        {"id": 4, "mycol": "C"},
        {"id": 5, "mycol": "C"},
    ]

    garbagevals = ["C"]

    df = spark.createDataFrame(Row(**x) for x in data_list)

    df = null_out_values_array(df, "mycol", garbagevals)

    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "mycol": "A"},
        {"id": 2, "mycol": "B"},
        {"id": 3, "mycol": "B"},
        {"id": 4, "mycol": None},
        {"id": 5, "mycol": None},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)

