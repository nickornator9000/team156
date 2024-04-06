import os
import yaml
# API credemtials must be set before importing kaggle module
with open('config/config.yaml', 'r') as file:
        data = yaml.safe_load(file)
        os.environ['KAGGLE_USERNAME'] = data['username']
        os.environ['KAGGLE_KEY'] = data['apiKey']
        
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, expr, concat_ws, to_timestamp
from pyspark.sql.types import IntegerType, DateType
import datetime
import kaggle

def julian_to_gregorian(jdn):
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

def load_data():
    with open('config/config.yaml', 'r') as file:
        data = yaml.safe_load(file)
        
    select_columns_query = f"(SELECT {', '.join(data['colNames'])} FROM Fires) AS subquery"
    
    spark = SparkSession.builder \
           .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0') \
           .getOrCreate()
    
    df = spark.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', query=select_columns_query, url=f"jdbc:sqlite:{os.getcwd()}/FPA_FOD_20170508.sqlite") \
        .load()
    return df

def clean_data(df):
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

def filter_by_timestamp(df, start_timestamp, end_timestamp):
    filtered_df = df.filter(
        (col("DISCOVERY_DATE_TIME") >= start_timestamp) & 
        (col("DISCOVERY_DATE_TIME") <= end_timestamp)
    )
    return filtered_df

def filter_by_yaml_timestamp(df):
    with open('config/config.yaml', 'r') as file:
        data = yaml.safe_load(file)
        
    start_timestamp = datetime.datetime.fromisoformat(data['startdate'])
    end_timestamp = datetime.datetime.fromisoformat(data['enddate'])
    return filter_by_timestamp(df, start_timestamp, end_timestamp)

def download_dataset():
    # Define the dataset path on Kaggle
    dataset_path = 'rtatman/188-million-us-wildfires/2'

    # Define the path where you want to download the dataset
    download_path = download_path = os.getcwd()
    
    # Check if a key dataset file already exists to avoid re-downloading
    key_file_path = os.path.join(download_path, 'FPA_FOD_20170508.sqlite')
    if os.path.exists(key_file_path):
        print(f"Dataset already exists at {key_file_path}. Skipping download.")
        return

    # Make sure the download path exists
    os.makedirs(download_path, exist_ok=True)

    # Use the Kaggle API to download the dataset
    kaggle.api.dataset_download_files(dataset_path, path=download_path, unzip=True)
    print("Dataset downloaded successfully.")

download_dataset()
df = clean_data(load_data())
df.show(n=3, vertical=True)
