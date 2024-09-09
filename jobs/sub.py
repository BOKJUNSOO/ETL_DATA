# time series analysis with elasticsearch and kibana
# needs for merged data (data model1)
# write merged to ElasticSearch and MySQL

from pyspark.sql import SparkSession , Window
import pyspark.sql.functions as F
from pyspark.sql.functions import when

import sys
import argparse

from mses import Es , Ms
from base import *
from filter import *
from datetime import *
#
parser = argparse.ArgumentParser()
args = parser.parse_args()

spark = (
    SparkSession
    .builder
    .master("local")
    .appName("Rank_info")
    .config("spark.jars.packages" , 'mysql:mysql-connector-j-9.0.0.jar')
    #.config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
    #.config("spark.jars", "opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
    .getOrCreate()
)

args.spark = spark

# input file name
mergedData_file_name = ""

# load merged data for some date
file_path1 = f"/opt/bitnami/spark/data/{mergedData_file_name}.json"
df = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path1)

# data model_1
# select column to use
df = init_df(df)

# Categorization level range with Status
# indexing with key value : date - class
df = location_df(df)
pivot = TopClassFilter(args)
df = pivot.filter(df)   # -- data model 1

# write merged ranking data to MySQL
mysql1 = Ms("jdbc:mysql://172.21.80.1:3306/MapleRanking")
mysql1.write_to_mysql(df,"distribution")

# write merged ranking data to Elastic Search
es = Es("http://es:9200")