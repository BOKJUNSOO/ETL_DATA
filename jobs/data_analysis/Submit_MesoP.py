from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import *

# creat spark object
spark = (
    SparkSession.builder
    .appName("Meso_potential")
    .master("local")
    .getOrCreate()
)

# load
file_path = sys.argv[1]
df = spark.read.format("json") \
               .option("multiLine",True) \
               .option("header", True) \
               .option("inferschema", True) \
               .load(file_path)

df.printSchema()

# initializing
# solve nested json with explode
df_flat = df.select(F.explode("potential_history")
                     .alias("potential_info"))
df_flat.show(1,False)

df_flat = df_flat.select("potential_info")

df_flat.show(10, False)

# extract each line of value column
df_flat = df_flat.withColumn("first_line_value", df_flat["value"].getItem(0))
df_flat = df_flat.withColumn("second_line_value", df_flat["value"].getItem(1))
df_flat = df_flat.withColumn("third_line_value", df_flat["value"].getItem(2))


# extract each line of grade column
df_flat = df_flat.withColumn("second_line_grade", df_flat["grade"].getItem(1))
df_flat = df_flat.withColumn("third_line_grade", df_flat["grade"].getItem(2))

df_flat = df_flat.drop("value","grade")
df_flat.show(10,False)



# filter
target_item = "마이스터 이어링"
target_potential_option = "레전드리"

# 첫줄에 나온 distinct한 option을 count
distinct_count_df = df_flat.where((F.col("target_item") == target_item))
distinct_count_df = distinct_count_df.where((F.col("potential_option_grade") == target_potential_option))
distinct_count_df = distinct_count_df.groupBy("first_line_value") \
                           .count() 
distinct_count_df = distinct_count_df.sort(F.desc("count")) \
                                     .limit(10)
distinct_count_df.show()

# 두번째줄에 나온 옵션이 이탈인 횟수를 count
break_count2_df = df_flat.where((F.col("target_item") == target_item))
break_count2_df = break_count2_df.where((F.col("potential_option_grade") == target_potential_option))
break_count2_df = break_count2_df.groupBy("second_line_grade") \
                                 .count()
break_count2_df.show()

# 세번째줄에 나온 옵션이 이탈인 횟수를 count
break_count3_df = df_flat.where((F.col("target_item") == target_item))
break_count3_df = break_count3_df.where((F.col("potential_option_grade") == target_potential_option))
break_count3_df = break_count3_df.groupBy("third_line_grade") \
                         .count()
break_count3_df.show()


# docker exec -it etl_data-spark-master-1 spark-submit --master spark://spark-master:7077 jobs/SparkSubmit.py date/myapi_m_2024-05-05.json