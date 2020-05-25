from pyspark.sql.functions import expr
from pyspark.sql.functions import date_format, to_timestamp

def normalise_dob(df, dob_col, date_fmt_if_string="yyyy-MM-dd", drop_orig=True):
    
    dtypes = dict(df.dtypes)
    
    if dtypes[dob_col] == "date":
        df = df.withColumn("dob_norm", date_format(dob_col,"yyyy-MM-dd"))
        
    if dtypes[dob_col] == "timestamp":
        df = df.withColumn("dob_norm", date_format(dob_col,"yyyy-MM-dd"))
    
    if dtypes[dob_col] == "string":
        df = df.withColumn("dob_norm", to_timestamp(dob_col, date_fmt_if_string))
        df = df.withColumn("dob_norm", date_format("dob_norm","yyyy-MM-dd"))

    if drop_orig:
        if dob_col != "dob_norm":
            df = df.drop(dob_col)

    return df 

def null_suspicious_dob_norm(df, dob_col):

    case_stmt = """
        case 
        when dob_norm = "1900-01-01" then null 
        when dob_norm = "1970-01-01" then null 
        else dob_norm end

    """
    
    df = df.withColumn('dob_norm', expr(case_stmt))
    
    return df  
