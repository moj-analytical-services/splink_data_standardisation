import pytest
import pandas as pd

from splink_data_normalisation.fix_string import fix_zero_length_strings, remove_leading_zeros
from pyspark.sql import Row


def test_fix_1(spark):

    names_list = [
        {"id": 1, "first_name": "", "surname": "a"},
        {"id": 2, "first_name": " ", "surname": "b"},
        {"id": 3, "first_name": " john", "surname": None},
    ]

    df = spark.createDataFrame(Row(**x) for x in names_list)
    df = df.select(list(names_list[0].keys()))

    df = fix_zero_length_strings(df)
    
    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "first_name": None, "surname": "a"},
        {"id": 2, "first_name": None, "surname": "b"},
        {"id": 3, "first_name": "john", "surname": None},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result,df_expected)

def test_fix_2(spark):

    data_list = [
        {"id": 1, "id_var": "00123"},
        {"id": 2, "id_var": "0abc"},
        {"id": 3, "id_var": " 0def"},
        {"id": 4, "id_var": "0001000"},
    ]


    df = spark.createDataFrame(Row(**x) for x in data_list)
    df = remove_leading_zeros(df, "id_var", "id_var_norm")

    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "id_var_norm": "123"},
        {"id": 2, "id_var_norm": "abc"},
        {"id": 3, "id_var_norm": " 0def"},
        {"id": 4, "id_var_norm": "1000"},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result,df_expected)