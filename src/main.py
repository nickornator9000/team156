#coordinate tasks
import modelData
from build.scripts.configSingleton import SingletonClass
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.feature import StandardScaler
from processData import clean_data

def load_data(columns:list)->DataFrame:
    select_columns_query = f"(SELECT {', '.join(columns)} FROM Fires) AS subquery"
    
    spark = SparkSession.builder \
           .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0')\
           .getOrCreate()
    
    df = spark.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', query=select_columns_query, url='jdbc:sqlite:data/FPA_FOD_20170508.sqlite') \
        .load()
    return df

if __name__ == "__main__":
    #get configurations needed
    getconfigs = SingletonClass.getConfig()
    k=40
    startdate = getconfigs['startdate']
    enddate = getconfigs['enddate']
    df = clean_data(load_data(getconfigs['colNames']))
    df = df.orderBy("DISCOVERY_DATE_TIME",ascending=False)
    df.show(3,vertical=True)
    df = df.select(['LATITUDE','LONGITUDE','OWNER_CODE','FIRE_SIZE','STAT_CAUSE_CODE','DISCOVERY_DOY'])
    
    
    """
    df = modelData.getFeatureVector(df)
    #df = modelData.scaleFeatureVector(df)
    df = modelData.runK_Means(df,featureCol="features",k=k)
    df.show(3)
    evaluator = ClusteringEvaluator()
    
    eval = evaluator.evaluate(df)
    
    print(str(eval))
    df = df.select(['LATITUDE','LONGITUDE','OWNER_CODE','FIRE_SIZE','STAT_CAUSE_CODE','DISCOVERY_DOY','prediction'])
    df = df.coalesce(1)
    df.write.csv("prototype_data.csv", header=True, mode="overwrite")
    df = df.groupBy("prediction").count()
    df.show(k)
    """
    df = df.withColumn("id",monotonically_increasing_id())
    tmp_df = df.select(['OWNER_CODE','FIRE_SIZE','STAT_CAUSE_CODE','DISCOVERY_DOY'])
    pca_features = modelData.getFeatureVector(tmp_df)

    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaler_fit = scaler.fit(pca_features)
    pca_features = scaler_fit.transform(pca_features)
    pca_features.show(10)

    pca_df = modelData.dimensionalityReduction(pca_features,2)
    pca_df = pca_df.withColumn("id",monotonically_increasing_id())
    df = df.join(pca_df,on='id')
    df = df.select(['LATITUDE','LONGITUDE','reduced_features'])
    df.show(10)
    df = modelData.getFeatureVector(df)
    df = modelData.runK_Means(df,"reduced_features",k)
    evaluator = ClusteringEvaluator()
    eval = evaluator.evaluate(df)
    print(str(eval))
    df = df.select(['LATITUDE','LONGITUDE','prediction'])
    df = df.coalesce(1)
    df.write.csv("prototype_data.csv", header=True, mode="overwrite")
    df = df.groupBy("prediction").count()
    df.show(k)

    