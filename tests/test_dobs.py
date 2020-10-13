import datetime

import pytest
import pandas as pd

from splink_data_standardisation.date_of_birth import normalise_dob
from pyspark.sql import Row

def test_dob_1(spark):


    dt = datetime.datetime(2020, 5, 25, 8, 5, 44, 815715)
    date = dt.date()
    date_str = date.strftime("%Y-%m-%d")
    date_str_alt = date.strftime("%d/%m/%Y")

    names_list = [
            {"dob_datetime": dt, "dob_date": date,"dob_str":date_str},
            {"dob_datetime": dt, "dob_date": date,"dob_str":date_str_alt},
            {"dob_datetime": None, "dob_date": None,"dob_str":None}
            ]

    df = spark.createDataFrame(Row(**x) for x in names_list)

    expected = [
        {"dob_norm": "2020-05-25"},
        {"dob_norm": "2020-05-25"},
        {"dob_norm": None}
        ]
    df_expected = pd.DataFrame(expected)
    df2 = normalise_dob(df.select("dob_datetime"), "dob_datetime")
    df_result = df2.toPandas()

    pd.testing.assert_frame_equal(df_result,df_expected)

    expected = [
        {"dob_norm": "2020-05-25"},
        {"dob_norm": "2020-05-25"},
        {"dob_norm": None}
        ]
    df_expected = pd.DataFrame(expected)
    df2 = normalise_dob(df.select("dob_date"), "dob_date")
    df_result = df2.toPandas()

    pd.testing.assert_frame_equal(df_result,df_expected)


    expected = [
        {"dob_norm": "2020-05-25"},
        {"dob_norm": None},
        {"dob_norm": None}
        ]
    df_expected = pd.DataFrame(expected)
    df2 = normalise_dob(df.select("dob_str"), "dob_str")
    df_result = df2.toPandas()

    pd.testing.assert_frame_equal(df_result,df_expected)


    expected = [
        {"dob_norm": None},
        {"dob_norm": "2020-05-25"},
        {"dob_norm": None}
        ]
    df_expected = pd.DataFrame(expected)
    df2 = normalise_dob(df.select("dob_str"), "dob_str", "dd/MM/yyyy")
    df_result = df2.toPandas()

    pd.testing.assert_frame_equal(df_result,df_expected)
