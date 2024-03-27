#coordinate tasks
import modelData
#from build.scripts.configSingleton import SingletonClass
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from processData import clean_data
from yaml import safe_load

def load_data():
    with open('config/config.yaml', 'r') as file:
        data = safe_load(file)
        
    select_columns_query = f"(SELECT {', '.join(data['colNames'])} FROM Fires) AS subquery"
    
    spark = SparkSession.builder \
           .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0')\
           .getOrCreate()
    
    df = spark.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', query=select_columns_query, url='jdbc:sqlite:data/FPA_FOD_20170508.sqlite') \
        .load()
    return df

if __name__ == "__main__":
    #do stuff
    print("Hello world")
    df = clean_data(load_data())
    df = df.select(['LATITUDE','LONGITUDE','OWNER_CODE','FIRE_SIZE','STAT_CAUSE_CODE'])
    df = modelData.getFeatureVector(df)
    df = modelData.runK_Means(df,k=10)
    df.show(3)