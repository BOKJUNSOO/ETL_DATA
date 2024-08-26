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
file_path = "/opt/bitnami/spark/data/ranking_2024-08-09.json"
df = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path)

df.printSchema() 

# select column to use
df_flat = df.select(F.explode("ranking")
                     .alias("ranking_info"))

df_flat = df_flat.select("ranking_info.date",
                         "ranking_info.character_name",
                         "ranking_info.character_level",
                         "ranking_info.class_name",
                         "ranking_info.sub_class_name")
df_flat.show(10,False) 

# fill class
df_flat = df_flat.withColumn("class" , df_flat["sub_class_name"])
df_flat = df_flat.withColumn("class",
                             F.when(F.col("sub_class_name") == "", df_flat["class_name"])\
                              .otherwise(F.col("class")))
df_flat = df_flat.drop("class_name","sub_class_name")
df_flat.show(10,False) 

# Categorization level range
df_flat = df_flat.withColumn("status" ,
                             when(df_flat["character_level"] > 289, "Tallahart")
                             .when((df_flat["character_level"] < 290) &(df_flat["character_level"] > 284) , "Carcion")
                             .when((df_flat["character_level"] < 285) & (df_flat["character_level"] > 279) , "Arteria")
                             .when((df_flat["character_level"] < 280) & (df_flat["character_level"] > 274) , "Dowonkyung")
                             .otherwise(F.col("character_level")))

df_flat.show(10,False) 

# groupBy level range
df_group = df_flat.groupBy("class","date") \
                  .pivot("status").count()
df_group.show(10,False) 

# sum Carcion + Arteria + Dowonkyung
map_list = ["Tallahart", "Carcion", "Arteria", "Dowonkyung"] 

df_group = df_group.withColumn("sum",
                               sum([F.col(c) for c in map_list]))
df_group.show(10,False) 

# set a key value
df_group = df_group.withColumn("key_value",
                               (F.concat(
                                   F.col("date"),
                                   F.lit("-"),
                                   F.col("class")
                                   )
                               ))
df = df_group.select(["class",
                      "Tallahart","Carcion","Arteria","Dowonkyung",
                      "sum","rank","date"]) 

# over partition by Date & order by sum
window = Window.partitionBy("date").orderBy(F.desc("sum"))
df = df.withColumn("rank", F.rank().over(window))
df.show(10, False)  


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
