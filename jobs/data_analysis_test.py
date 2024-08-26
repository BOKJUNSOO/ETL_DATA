from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import when
import sys
from datetime import *

#
spark = (
    SparkSession
    .builder
    .master("local")
    .appName("Rank_info")
    .config("spark.jars", "/opt/bitnami/spark/resources/mysql-connector-j-9.0.0.jar")
    .getOrCreate()
)

# load
# file_path = f"/opt/bitnami/spark/data/ranking_{target_date}-*.json"
file_path = "/opt/bitnami/spark/data/ranking_2024-06-11.json"
df = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path)

"""init_df method"""
# select column to use
df_flat = df.select(F.explode("ranking")
                     .alias("ranking_info"))

df_flat = df_flat.select("ranking_info.date",
                         "ranking_info.character_name",
                         "ranking_info.character_level",
                         "ranking_info.class_name",
                         "ranking_info.sub_class_name")

# fill class
df_flat = df_flat.withColumn("class" , df_flat["sub_class_name"])
df_flat = df_flat.withColumn("class",
                             F.when(F.col("sub_class_name") == "", df_flat["class_name"])\
                              .otherwise(F.col("class")))
df_flat = df_flat.drop("class_name","sub_class_name")

# Categorization level range
df_flat = df_flat.withColumn("status" ,
                             when(df_flat["character_level"] > 289, "Tallahart")
                             .when((df_flat["character_level"] < 290) &(df_flat["character_level"] > 284) , "Carcion")
                             .when((df_flat["character_level"] < 285) & (df_flat["character_level"] > 279) , "Arteria")
                             .when((df_flat["character_level"] < 280) & (df_flat["character_level"] > 274) , "Dowonkyung")
                             .otherwise(F.col("character_level")))


"""TopClassFilter method"""
# groupBy level range


df_flat = df_flat.groupBy("class","date") \
                  .pivot("status").count()


# sum Carcion + Arteria + Dowonkyung
map_list = ["Tallahart", "Carcion", "Arteria", "Dowonkyung"] 

df_flat = df_flat.withColumn("sum",
                               sum([F.col(c) for c in map_list]))


# over partition by Date & order by sum
window = Window.partitionBy("date").orderBy(F.desc("sum"))
df_flat = df_flat.withColumn("rank", F.rank().over(window))


# set a key value --> 데이터 시각화 키값 기준(data analysis)
df_flat = df_flat.withColumn("key_value",
                               (F.concat(
                                   F.col("date"),
                                   F.lit("-"),
                                   F.col("class")
                                   )
                               ))


df_flat = df_flat.select("class",
                         "sum",
                         "rank",
                         "Arteria",
                         "Carcion",
                         "Dowonkyung",
                         "Tallahart",
                         "key_value")

df_flat.show(20, False)
# analysis topic 1) before limbo vs after limbo
# increase ratio of magition
# increase ratio of dealer with high rate
# analysis topic 2) class change count

# Save DataFrame by write data to the MySQL Server
"""df.write \
  .format("jdbc") \
  .option("driver", "com.mysql.cj.jdvc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/emp") \
  .option("dbtable", "ranking") \
  .option("user", "root") \
  .option("password", "") \
  .save()
"""
