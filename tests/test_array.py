import pytest
import pandas as pd

from splink_data_standardisation.arrays import (
    fix_zero_length_arrays,
    remove_leading_zeros_array,
)
from pyspark.sql import Row


def test_fix_1(spark):

    names_list = [
        {"id": 1, "my_arr1": ["a", "b", "c"], "other_arr": [], "my_str": "a"},
        {"id": 2, "my_arr1": [], "other_arr": [1], "my_str": "a"},
        {"id": 3, "my_arr1": [None, "", "c"], "other_arr": [1], "my_str": "a"},
        {"id": 4, "my_arr1": [None, ""], "other_arr": [1], "my_str": "a"},
        {"id": 5, "my_arr1": [""], "other_arr": [1], "my_str": "a"},
        {"id": 6, "my_arr1": [None, None], "other_arr": [1], "my_str": "a"},
    ]

    df = spark.createDataFrame(Row(**x) for x in names_list)
    df = df.select(list(names_list[0].keys()))
    df = fix_zero_length_arrays(df)

    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "my_arr1": ["a", "b", "c"], "other_arr": None, "my_str": "a"},
        {"id": 2, "my_arr1": None, "other_arr": [1], "my_str": "a"},
        {"id": 3, "my_arr1": ["c"], "other_arr": [1], "my_str": "a"},
        {"id": 4, "my_arr1": None, "other_arr": [1], "my_str": "a"},
        {"id": 5, "my_arr1": None, "other_arr": [1], "my_str": "a"},
        {"id": 6, "my_arr1": None, "other_arr": [1], "my_str": "a"},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)


def test_leading_zeros_array_1(spark):

    data_list = [
        {"col1": ["a", "b", "00001"]},
        {"col1": ["b", "000002", "d"]},
        {"col1": ["03", "d", "e"]},
        {"col1": ["d", "001", "f"]},
    ]

    df = spark.createDataFrame(Row(**x) for x in data_list)
    df = remove_leading_zeros_array(df, "col1")

    df_result = df.toPandas()

    df_expected = [
        {"col1": ["a", "b", "1"]},
        {"col1": ["b", "2", "d"]},
        {"col1": ["3", "d", "e"]},
        {"col1": ["d", "1", "f"]},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)
