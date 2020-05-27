import pytest
import pandas as pd

from splink_data_normalisation.postcode import postcode_to_inward_outward
from pyspark.sql import Row


def test_pc_1(spark):

    postcode_list = [
        {"id": 1, "postcode": "AB1 2CD"},
        {"id": 2, "postcode": "AB12CD"},
        {"id": 3, "postcode": "AB12 3CD"},
        {"id": 4, "postcode": "AB123CD"},
        {"id": 5, "postcode": "AB1C 2DE"},
        {"id": 6, "postcode": "AB1C2DE"},
        {"id": 7, "postcode": "  ab1 2CD "},
    ]

    df = spark.createDataFrame(Row(**x) for x in postcode_list)
    
    df = postcode_to_inward_outward(df, "postcode")
    df_result = df.toPandas()
    
    expected = [
        {"id": 1, "outward_postcode_norm": "AB1", "inward_postcode_norm": "2CD"},
        {"id": 2, "outward_postcode_norm": "AB1", "inward_postcode_norm": "2CD"},
        {"id": 3, "outward_postcode_norm": "AB12", "inward_postcode_norm": "3CD"},
        {"id": 4, "outward_postcode_norm": "AB12", "inward_postcode_norm": "3CD"},
        {"id": 5, "outward_postcode_norm": "AB1C", "inward_postcode_norm": "2DE"},
        {"id": 6, "outward_postcode_norm": "AB1C", "inward_postcode_norm": "2DE"},
        {"id": 7, "outward_postcode_norm": "AB1", "inward_postcode_norm": "2CD"}
    ]

    df_expected = pd.DataFrame(expected)

    pd.testing.assert_frame_equal(df_result,df_expected)