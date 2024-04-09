#coordinate tasks
import modelData
from build.scripts.configSingleton import SingletonClass
from pyspark.ml.evaluation import ClusteringEvaluator
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
    df = df.orderBy("DISCOVERY_DATE_TIME",ascending=False)
    df.show(3,vertical=True)
    df = df.select(['LATITUDE','LONGITUDE','OWNER_CODE','FIRE_SIZE','STAT_CAUSE_CODE','DISCOVERY_DOY'])
    #df = df.limit(200_000)
    df = modelData.getFeatureVector(df)
    df = modelData.runK_Means(df,k=20)
    df.show(3)
    evaluator = ClusteringEvaluator()
    
    eval = evaluator.evaluate(df)
    
    print(str(eval))
    df = df.select(['LATITUDE','LONGITUDE','prediction'])
    df = df.coalesce(1)
    df.write.csv("prototype_data.csv", header=True, mode="overwrite")
    df = df.groupBy("prediction").count()
    df.show(30)