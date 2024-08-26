from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import *

#
spark = (
    SparkSession.builder
    .appName("Cube_potential")
    .master("local")
    .getOrCreate()
)

# load
file_path = sys.argv[1]
df = spark.read.format("json") \
               .option("multiLine", True) \
               .option("header" , True) \
               .option("inferschema" , True) \
               .load(file_path)

df.printSchema()

# initializing
df_flat = df.select(F.explode("cube_history")   # to object
                     .alias("cube_info"))
df_flat.show(1, False)

df_flat = df_flat.select(
    "cube_info.cube_type",
    "cube_info.target_item",
    "cube_info.additional_potential_option_grade",
    "cube_info.after_additional_potential_option.value",
    "cube_info.after_additional_potential_option.grade"
)

df_flat.show(10, False)

# extract each line of value column
df_flat = df_flat.withColumn("first_line_value", df_flat["value"].getItem(0))
df_flat = df_flat.withColumn("second_line_value", df_flat["value"].getItem(1))
df_flat = df_flat.withColumn("third_line_value", df_flat["value"].getItem(2))

# extract each line of grade column
df_flat = df_flat.withColumn("second_line_grade", df_flat["grade"].getItem(1))
df_flat = df_flat.withColumn("second_line_grade", df_flat["grade"].getItem(2))

# sorting cube/ potential / target item
target_item = "제네시스 스태프"
target_potential_option = "레전드리"

# 첫줄에 나온 distinct한 option을 count
distinct_count_df = df_flat.where((F.col("target_item") == target_item))
distinct_count_df = distinct_count_df.where(F.col("additional_potential_option_grade") == target_potential_option)
distinct_count_df = distinct_count_df.groupBy("first_line_value") \
                           .count()                          
distinct_count_df = distinct_count_df.sort(F.desc("count")) \
                                     .limit(10)
distinct_count_df.show()

# 화이트 에디셔널 큐브 사용기록
target_cube = "화이트 에디셔널 큐브"

target_cube_hitory = df_flat.where((F.col("cube_type") == target_cube))
target_cube_hitory = target_cube_hitory.groupBy("first_line_value") \
                                       .count()
target_cube_hitory = target_cube_hitory.sort(F.desc("count")) \
                                       .limit(10)
target_cube_hitory.show()