from pyspark.sql.functions import expr
from pyspark.sql.functions import date_format, to_timestamp
from pyspark.sql.dataframe import DataFrame


def normalise_dob(
    df: DataFrame,
    dob_col: str,
    date_fmt_if_string: str = "yyyy-MM-dd",
    drop_orig: bool = True,
):
    """Create column called dob_norm with dob as a string in yyyy-MM-dd format
    or null otherwise

    Args:
        df (DataFrame): Spark dataframe
        dob_col (str): Name of dob column
        date_fmt_if_string (str, optional): Date format if incoming dates are already string. Defaults to "yyyy-MM-dd".
        drop_orig (bool, optional): Drop original date of birth column. Defaults to True.

    Returns:
        DataFrame: Spark DataFrame with new normalised dob column called dob_norm
    """

    dtypes = dict(df.dtypes)

    if dtypes[dob_col] == "date":
        df = df.withColumn("dob_norm", date_format(dob_col, "yyyy-MM-dd"))

    if dtypes[dob_col] == "timestamp":
        df = df.withColumn("dob_norm", date_format(dob_col, "yyyy-MM-dd"))

    if dtypes[dob_col] == "string":
        df = df.withColumn("dob_norm", to_timestamp(dob_col, date_fmt_if_string))
        df = df.withColumn("dob_norm", date_format("dob_norm", "yyyy-MM-dd"))

    if drop_orig:
        if dob_col != "dob_norm":
            df = df.drop(dob_col)

    return df


def null_suspicious_dob_norm(df: DataFrame, dob_col: str = "dob_norm"):
    """Null out suspicious dates of birth

    Args:
        df (DataFrame): Input Spark DataFrame.  Expects that dob column has already been normalised
        dob_col (str, optional): Name of normalised dob col. Defaults to "dob_norm".

    Returns:
        DataFrame: Original dataframe with suspicious dates of birth nulled out 
    """    

    case_stmt = """
        case 
        when dob_norm = "1900-01-01" then null 
        when dob_norm = "1970-01-01" then null 
        else dob_norm end

    """
    df = df.withColumn(dob_col, expr(case_stmt))

    return df

