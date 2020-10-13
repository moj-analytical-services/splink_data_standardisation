import pytest
import pandas as pd

from splink_data_standardisation.freq import null_out_entries_with_freq_above_n
from pyspark.sql import Row


def test_freq_1(spark):


    data_list = [
    {"id": 1, "mycol": "A"},
    {"id": 2, "mycol": "B"},
    {"id": 3, "mycol": "B"},
    {"id": 4, "mycol": "C"},
    {"id": 5, "mycol": "C"},
    {"id": 6, "mycol": "C"},
    {"id": 7, "mycol": None},


    ]

    df = spark.createDataFrame(Row(**x) for x in data_list)

    df = null_out_entries_with_freq_above_n(df, 'mycol', 2, spark)

    df_result = df.toPandas()

    df_expected = [
    {"id": 1, "mycol": "A"},
    {"id": 2, "mycol": "B"},
    {"id": 3, "mycol": "B"},
    {"id": 4, "mycol": None},
    {"id": 5, "mycol": None},
    {"id": 6, "mycol": None},
    {"id": 7, "mycol": None},
    ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result,df_expected)