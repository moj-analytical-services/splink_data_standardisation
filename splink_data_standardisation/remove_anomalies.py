from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f


def null_out_values(df: DataFrame, colname: str, values_to_null):
    """Null out a list of undesirable values in a column
    Useful for columns that mostly contain valid data but occasionally
    contain other values such as 'unknown'
    Args:
        df (DataFrame): The dataframe to clean
        colname (string): The name of the column to clean
        values_to_null: A list of values to be nulled.

    Returns:
        DataFrame: The cleaned dataframe with incoming column overwritten
    """

    if len(values_to_null) == 0:
        return df

    values_to_null_string = [f'"{v}"' for v in values_to_null]
    values_to_null_joined = ", ".join(values_to_null_string)

    case_statement = f"""
    CASE
    WHEN {colname} in ({values_to_null_joined}) THEN NULL
    ELSE {colname}
    END
    """

    df = df.withColumn(colname, f.expr(case_statement))

    return df


def null_out_values_array(df: DataFrame, array_colname: str, values_to_null):
    """Null out a list of undesirable values in a column that contains an array of values
    Useful for columns that mostly contain valid data but occasionally
    contain other values such as 'unknown'
    if column is not an array it falls back to null_out_values
    
    Args:
        df (DataFrame): The dataframe to clean
        colname (string): The name of the column to clean
        values_to_null: A list of values to be nulled.
    Returns:
        DataFrame: The cleaned dataframe with column containing array that has values in values_to_null nulled
    """

    if len(values_to_null) == 0:
        return df

    values_to_null_string = [f'"{v}"' for v in values_to_null]
    values_to_null_joined = ", ".join(values_to_null_string)

    mappingstr = f"""
    CASE WHEN x in ({values_to_null_joined}) THEN NULL
    ELSE x
    END"""

    sqlstr = f"""
    TRANSFORM({array_colname}, x -> {mappingstr}) 
    """

    if str((dict(df.dtypes)[array_colname])).startswith("array"):
        df = df.withColumn(array_colname, f.expr(sqlstr))
    else:
        # if column is not an array fall back to null_out_values
        df = null_out_values(df, array_colname, values_to_null)

    return df
