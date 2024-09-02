# time series analysis with elasticsearch and kibana
# needs for merged data (data model1)

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


# load merged data for some date
file_path1 = "/opt/bitnami/spark/data/ranking_data_8.json"
df = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path1)


# load exp data
#args.input_path2 = "/opt/bitnami/spark/data/maple_exp.csv"
#df_e = read_input_csv(args.spark, args.input_path2)

# data model_1
# select column to use
df = init_df(df)

# Categorization level range with Status
# indexing with key value : date - class
df = location_df(df)
#
pivot = TopClassFilter(args)
df = pivot.filter(df)
df.show(10,False)
"""데이터 용량이 너무 커지면 mysql, es저장에 문제가 있음
2달치 데이터는 submit도 안됨..."""
# write daily ranking data to elasticsearch
# test for kibana

es = Es("http://es:9200")
es.write_elasticesearch(df,f"ranking_data_2024_8")


# write data to elastic search for kibana
# analysis topic - before limbo vs after limbo (change ratio of some class)
# analysis topic2 - hunting rank (top 10 rate?)
