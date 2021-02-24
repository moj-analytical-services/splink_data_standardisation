from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, regexp_replace, col

# This fixes a problem where athena can't handle a parquet file with a zero length array
# so [None] is fine, and so is None, but [] is not
# See here: https://forums.aws.amazon.com/thread.jspa?messageID=874178&tstart=0
# A reprex is here https://gist.github.com/RobinL/0692e2cd266483b3088646206aa8be62
def fix_zero_length_arrays(df: DataFrame):
    """For every field of type array, turn zero length arrays into true nulls

    Args:
        df (DataFrame): Input Spark dataframe

    Returns:
        DataFrame: Spark Dataframe with clean arrays
    """

    array_cols_of_strings = []
    other_array_cols = []

    for item in df.dtypes:
        if item[1].startswith("array"):
            if item[1] == "array<string>":
                array_cols_of_strings.append(item[0])
            else:
                other_array_cols.append(item[0])

    for c in array_cols_of_strings:
        df = fix_zero_length_array(c, df, col_is_string=True)
    for c in other_array_cols:
        df = fix_zero_length_array(c, df, col_is_string=False)

    return df


def fix_zero_length_array(column_name: str, df: DataFrame, col_is_string=False):
    """Turn zero length arrays into true nulls for a single column

    Args:
        column_name (str): Col name
        df (DataFrame): Dataframe in which the column resides
    """

    if col_is_string:
        stmt = f"""
        case
        when size(filter({column_name}, x -> x is not null and trim(x) != '')) > 0
        then filter({column_name}, x -> x is not null and trim(x) != '')
        else null
        end
        """
    else:
        stmt = f"""
        case
        when size(filter({column_name}, x -> x is not null)) > 0
        then filter({column_name}, x -> x is not null)
        else null
        end
        """

    df = df.withColumn(column_name, expr(stmt))
    return df