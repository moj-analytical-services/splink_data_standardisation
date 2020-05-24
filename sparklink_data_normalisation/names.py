from pyspark.sql.functions import expr

def normalise_names(df, name_cols, drop_orig=True):
    name_col_joined = ", ".join(name_cols)
    print(name_col_joined)
    df = df.withColumn('name_concat', expr(f"concat_ws(' ', {name_col_joined})"))
    df = df.withColumn('name_concat', expr("regexp_replace(name_concat, '[\\-\\.]', ' ')"))
    df = df.withColumn('name_arr', expr("split(name_concat, ' ')"))
    df = df.withColumn('surname_norm', expr("case when surname is not null then element_at(name_arr,-1) else null end"))
    df = df.withColumn('forename1_norm', expr("case when size(name_arr) > 1 then element_at(name_arr,1) else null end"))
    df = df.withColumn('forename2_norm', expr("case when size(name_arr) > 2 then element_at(name_arr,2) else null end"))
    df = df.withColumn('forename3_norm', expr("case when size(name_arr) > 3 then element_at(name_arr,3) else null end"))
    df = df.withColumn('forename4_norm', expr("case when size(name_arr) > 4 then element_at(name_arr,4) else null end"))
    df = df.withColumn('forename5_norm', expr("case when size(name_arr) > 5 then element_at(name_arr,5) else null end"))
    df = df.drop("name_arr", "name_concat")
    if drop_orig:
        for n in name_cols:
            df = df.drop(n)
    return df 