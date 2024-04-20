#coordinate tasks
import modelData
from build.scripts.configSingleton import SingletonClass
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession,DataFrame
from pyspark.ml.feature import StandardScaler
from processData import clean_data
import os
import sys

def load_data(columns:list)->DataFrame:
    select_columns_query = f"(SELECT {', '.join(columns)} FROM Fires) AS subquery"
    
    spark = SparkSession.builder \
            .master('local[*]')\
           .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0')\
           .getOrCreate()
    
    df = spark.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', query=select_columns_query, url='jdbc:sqlite:data/FPA_FOD_20170508.sqlite') \
        .load()
    return df

if __name__ == "__main__":
    #get configurations needed
    getconfigs = SingletonClass.getConfig()
    k=20
    startdate = getconfigs['startdate']
    enddate = getconfigs['enddate']
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    df = clean_data(load_data(getconfigs['colNames']))
    df = df.orderBy("DISCOVERY_DATE_TIME",ascending=False)
    df = df.select(['LATITUDE','LONGITUDE','OWNER_CODE','FIRE_SIZE','STAT_CAUSE_CODE','DISCOVERY_DOY'])

    df = modelData.getFeatureVector(df)
    #df = modelData.scaleFeatureVector(df)
    df = modelData.dimensionalityReduction(df,3,inputCol='features')
    #df = modelData.getFeatureVector(df,inputCols=['LATITUDE','LONGITUDE','reduced_features'],outputCol="final_features")
    df = modelData.runK_Means(cleanedData=df,featureCol='reduced_features',k=k)
    df.show(10)

    evaluator = ClusteringEvaluator()
    eval = evaluator.evaluate(df)
    print(str(eval))
    df=df.drop('features')
    df=df.drop('reduced_features')
    df = df.coalesce(1)
    df.write.csv("data/model_output.csv", header=True, mode="overwrite")

    df = df.groupBy("prediction").count()
    df.show(k)
    

    