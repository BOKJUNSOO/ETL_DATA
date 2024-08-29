# time series analysis with elasticsearch and kibana

from pyspark.sql import SparkSession , Window
import pyspark.sql.functions as F
from pyspark.sql.functions import when

import sys
import argparse

from mses import Es , Ms
from base import *
from filter import *
#
parser = argparse.ArgumentParser()
args = parser.parse_args()

spark = (
    SparkSession
    .builder
    .master("local")
    .appName("Rank_info")
    #.config("spark.jars.packages" , 'mysql:mysql-connector-j-9.0.0.jar')
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
    .config("spark.jars", "opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
    .getOrCreate()
)

# load merged data for some date
# before day of maple now 6/11 ~ 7/10
file_path1 = "/opt/bitnami/spark/data/ranking_B.json"
df1 = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path1)

# after day of maple now 7/11 ~ 8/11
file_path2 = "/opt/bitnami/spark/data/ranking_A.json"
df2 = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path2)

args.spark = spark
args.file_path1 = file_path1
args.file_path2 = file_path2
          

# select column to use
df1 = init_df(df1)
df2 = init_df(df2)

# Categorization level range with Status
# indexing with key value : date - class
df1 = location_df(df1)
df2 = location_df(df2)

#
pivot = TopClassFilter(args)
df1 = pivot.filter(df1)
df2 = pivot.filter(df2)

df1.show(10,False)
df2.show(10,False)

# test for kibana
# df1.select("sum").where("class" == "비숍").show(60,False)
# pyspark.errors.exceptions.base.PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `condition` should be a Column or str, got bool.


# Save DataFrame at RDBMS(MySQL) -- done
#mysql = Ms("jdbc:mysql://172.21.80.1:3306/MapleRanking")
#mysql.write_to_mysql(df1, "before_limbo_live")
#mysql.write_to_mysql(df2, "after_limbo_live")

# done!
#es = Es("http://es:9200")
#es.write_elasticesearch(df1, "before_limbo_maplenow")
#es.write_elasticesearch(df2, "after_limbo_maplenow")

# Save DataFrame by write data to elastic search for kibana
# analysis topic - before limbo vs after limbo (change ratio of some class)
