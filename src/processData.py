import os
from pyspark.sql.functions import col, udf, expr, concat_ws, to_timestamp
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import DataFrame
import datetime

def julian_to_gregorian(jdn:int)->datetime.date:
    if jdn is None:
        return None
    J = float(jdn) + 0.5
    j = J + 32044
    g = j // 146097
    dg = j % 146097
    c = (dg // 36524 + 1) * 3 // 4
    dc = dg - c * 36524
    b = dc // 1461
    db = dc % 1461
    a = (db // 365 + 1) * 3 // 4
    da = db - a * 365
    y = g * 400 + c * 100 + b * 4 + a
    m = (da * 5 + 308) // 153 - 2
    d = da - (m + 4) * 153 // 5 + 122
    Y = int(y - 4800 + (m + 2) // 12)
    M = int((m + 2) % 12 + 1)
    D = int(d + 1)
    return datetime.date(Y, M, D)

def clean_data(df:DataFrame)->DataFrame:
    column_cast_dict = {
        "FOD_ID": IntegerType(), 
        "FIRE_YEAR": IntegerType(), 
        "STAT_CAUSE_CODE": IntegerType(), 
        "OWNER_CODE": IntegerType(),
        "CONT_DOY": IntegerType(),
        "DISCOVERY_DOY": IntegerType()
    }
    
    julian_to_gregorian_udf = udf(julian_to_gregorian, DateType())
    
    for column_name, column_type in column_cast_dict.items():
        df = df.withColumn(column_name, col(column_name).cast(column_type))
        
    df = df.withColumn("DISCOVERY_DATE", julian_to_gregorian_udf(df["DISCOVERY_DATE"]))
    df = df.withColumn("CONT_DATE", julian_to_gregorian_udf(df["CONT_DATE"]))

    df = df.withColumn("DISCOVERY_TIME", expr("concat(substr(DISCOVERY_TIME, 1, 2), ':', substr(DISCOVERY_TIME, 3, 2), ':00')"))
    df = df.withColumn("CONT_TIME", expr("concat(substr(CONT_TIME, 1, 2), ':', substr(CONT_TIME, 3, 2), ':00')"))

    df = df.withColumn("DISCOVERY_DATE_TIME", to_timestamp(concat_ws(" ", "DISCOVERY_DATE", "DISCOVERY_TIME"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("CONT_DATE_TIME", to_timestamp(concat_ws(" ", "CONT_DATE", "CONT_TIME"), "yyyy-MM-dd HH:mm:ss"))
    return df

def filter_by_yaml_timestamp(df:DataFrame,startdate:str,enddate:str)->DataFrame:
    #pass dates in main class if you wish to use this feature via "getconfigs" dict
    start_timestamp = datetime.datetime.fromisoformat(startdate)
    end_timestamp = datetime.datetime.fromisoformat(enddate)
    filtered_df = df.filter(
        (col("DISCOVERY_DATE_TIME") >= start_timestamp) & 
        (col("DISCOVERY_DATE_TIME") <= end_timestamp)
    )
    return filtered_df
