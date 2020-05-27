from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr

def postcode_to_inward_outward(df: DataFrame, pc_field: str, drop_orig:bool = True):
    """Given a field containing a postcode, creates new columns in the dataframe
    called outward_postcode_norm and inward_postcode_norm

    Original postcode can have spaces or not and be in any case

    Args:
        df (DataFrame): Spark Dataframe
        pc_field (str): Name of field containing postcode
    """    

    sql = f"upper(replace({pc_field}, ' ', ''))"
    df = df.withColumn("pc_nospace_temp__", expr(sql))
    
    sql = f"""left(pc_nospace_temp__, length(pc_nospace_temp__) - 3)"""
    df = df.withColumn("outward_postcode_norm", expr(sql))

    sql = f"""right(pc_nospace_temp__, 3)"""
    df = df.withColumn("inward_postcode_norm", expr(sql))

   

    df = df.drop("pc_nospace_temp__")

    if drop_orig:
        df = df.drop(pc_field)

    return df