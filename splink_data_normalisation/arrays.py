from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, regexp_replace, col

# This fixes a problem where athena can't handle a parquet file with a zero length array 
# so [None] is fine, and so is None, but [] is not
# See here: https://forums.aws.amazon.com/thread.jspa?messageID=874178&tstart=0
# A reprex is here https://gist.github.com/RobinL/0692e2cd266483b3088646206aa8be62
def fix_zero_length_arrays(df:DataFrame):
    """For every field of type array, turn zero length arrays into true nulls

    Args:
        df (DataFrame): Input Spark dataframe

    Returns:
        DataFrame: Spark Dataframe with clean arrays
    """    

    array_cols = [item[0] for item in df.dtypes if item[1].startswith('array')]

    stmt = """
    case 
    when size(filter({c}, x -> x is not null and trim(x) != '')) > 0 then filter({c}, x -> x is not null and trim(x) != '')
    else null
    end
    """

    for c in array_cols:
        df = df.withColumn(c, expr(stmt.format(c=c)))

    return df
