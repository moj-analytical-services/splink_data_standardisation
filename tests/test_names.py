
import pytest
import pandas as pd 

from sparklink_data_normalisation.names import normalise_names 
from pyspark.sql import Row

def test_names_1(spark):

    names_list = [
        {"id":1, "first_name":"john","surname":"smith jones"},
        {"id":2, "first_name":"john","surname":"smith-jones"},
        {"id":3, "first_name":"john.smith","surname":"jones"}
        ]

    df = spark.createDataFrame(Row(**x) for x in names_list)
    df = normalise_names(df, ["first_name", "surname"])
    
    df_result = df.toPandas()

    df_expected = [
     {'id': 1, 'surname_norm': 'jones', 'forename1_norm': 'john', 'forename2_norm': 'smith', 'forename3_norm': None, 'forename4_norm': None, 'forename5_norm': None},
     {'id': 2, 'surname_norm': 'jones', 'forename1_norm': 'john', 'forename2_norm': 'smith', 'forename3_norm': None, 'forename4_norm': None, 'forename5_norm': None},
     {'id': 3, 'surname_norm': 'jones', 'forename1_norm': 'john', 'forename2_norm': 'smith', 'forename3_norm': None, 'forename4_norm': None, 'forename5_norm': None}
     ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result,df_expected)


    names_list = [
        {"id":1, "first_name":"john", "middle_name": "james peter", "surname":"smith-jones"},
        {"id":2, "first_name":"john", "middle_name": "james peter smith", "surname":"jones"},
        ]

    df = spark.createDataFrame(Row(**x) for x in names_list)
    df = normalise_names(df, ["first_name", "middle_name", "surname"])
    
    df_result = df.toPandas()

    df_expected = [
     {'id': 1, 'surname_norm': 'jones', 'forename1_norm': 'john', 'forename2_norm': 'james', 'forename3_norm': "peter", 'forename4_norm': "smith", 'forename5_norm': None},
     {'id': 2, 'surname_norm': 'jones', 'forename1_norm': 'john', 'forename2_norm': 'james', 'forename3_norm': "peter", 'forename4_norm': "smith", 'forename5_norm': None},
    
     ]

    df_expected = pd.DataFrame(df_expected)

    pd.testing.assert_frame_equal(df_result,df_expected)

    
