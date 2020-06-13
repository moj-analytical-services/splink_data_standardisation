import pytest
import pandas as pd

from splink_data_normalisation.arrays import fix_zero_length_arrays
from pyspark.sql import Row


def test_fix_1(spark):

    names_list = [
        {"id": 1, "my_arr1": ["a", "b", "c"], "other_arr": [ ],"my_str": "a"},
        {"id": 2, "my_arr1": [             ], "other_arr": [1],"my_str": "a"},

            ]

    df = spark.createDataFrame(Row(**x) for x in names_list)
    df = df.select(list(names_list[0].keys()))

    df = fix_zero_length_arrays(df)
    
    df_result = df.toPandas()

    df_expected = [
        {"id": 1, "my_arr1": ["a", "b", "c"], "other_arr": None,"my_str": "a"},
        {"id": 2, "my_arr1": None,            "other_arr": [1] ,"my_str": "a"},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result,df_expected)

