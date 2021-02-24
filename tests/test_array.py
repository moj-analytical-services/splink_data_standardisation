import pytest
import pandas as pd

from splink_data_standardisation.arrays import fix_zero_length_arrays
from pyspark.sql import Row


def test_fix_1(spark):

    # fmt: off
    names_list = [
        {"id": 1, "my_arr1": ["a", "b", "c"], "other_arr": [ ],"my_str": "a", "complex_arr": [[1], [1]]},
        {"id": 2, "my_arr1": [             ], "other_arr": [1],"my_str": "a", "complex_arr": []},
        {"id": 3, "my_arr1": [None, '', 'c'], "other_arr": [1],"my_str": "a", "complex_arr": None},
        {"id": 4, "my_arr1": [None, ''     ], "other_arr": [1],"my_str": "a", "complex_arr": [[1]]},
        {"id": 5, "my_arr1": [''           ], "other_arr": [1],"my_str": "a", "complex_arr": [[1],[1]]},
        {"id": 6, "my_arr1": [None, None   ], "other_arr": [1],"my_str": "a", "complex_arr": [[1],[1]]},
        ]
    # fmt: on

    df = spark.createDataFrame(Row(**x) for x in names_list)
    df = df.select(list(names_list[0].keys()))
    df = fix_zero_length_arrays(df)

    df_result = df.toPandas()

    # fmt: off
    df_expected = [
        {"id": 1, "my_arr1": ["a", "b", "c"], "other_arr": None,"my_str": "a", "complex_arr": [[1], [1]]},
        {"id": 2, "my_arr1": None,            "other_arr": [1] ,"my_str": "a", "complex_arr": None},
        {"id": 3, "my_arr1": ['c'],           "other_arr": [1] ,"my_str": "a", "complex_arr": None},
        {"id": 4, "my_arr1": None,            "other_arr": [1] ,"my_str": "a", "complex_arr": [[1]]},
        {"id": 5, "my_arr1": None,            "other_arr": [1] ,"my_str": "a", "complex_arr": [[1],[1]]},
        {"id": 6, "my_arr1": None,            "other_arr": [1] ,"my_str": "a", "complex_arr": [[1],[1]]},

    ]
    # fmt: on

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result, df_expected)
