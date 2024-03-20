from pyspark.sql import SparkSession
import sqlite3
con = sqlite3.connect("data/FPA_FOD_20170508.sqlite")

spark = SparkSession.builder\
           .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0')\
           .getOrCreate()

df = spark.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', dbtable='Fires',
                 url='jdbc:sqlite:data/FPA_FOD_20170508.sqlite')\
        .load()

df = df.drop(df.columns[-1])
df.show(5)
