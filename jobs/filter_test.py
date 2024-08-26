from pyspark.sql import SparkSession , Window
import pyspark.sql.functions as F
from pyspark.sql.functions import sum, max
from base import *
spark = (
    SparkSession.builder
    .master("local")
    .appName("exp_info")
    .getOrCreate()
)

# load yesterday ranking data
file_path = "/opt/bitnami/spark/data/ranking_2024-08-11.json"
df_b = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path)

# load today ranking data
file_path = "/opt/bitnami/spark/data/ranking_2024-08-12.json"
df = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path)

# load exp data
file_path = "/opt/bitnami/spark/data/maple_exp.csv"
df_e = spark.read \
             .format("csv") \
             .option("multiLine", True) \
             .option("header", True) \
             .option("inferschema", True) \
             .load(file_path)

df_e = df_e.select("level",
                   "need_exp")

# 1 "init_df" method
# select column to use
df1 = df_b.select(F.explode("ranking")
                     .alias("ranking_info"))

df1 = df1.select("ranking_info.date",
                 "ranking_info.character_name",
                 "ranking_info.character_level",
                 "ranking_info.character_exp",
                 "ranking_info.class_name",
                 "ranking_info.sub_class_name")

df2 = df.select(F.explode("ranking")
                     .alias("ranking_info"))

df2 = df2.select("ranking_info.date",
                 "ranking_info.character_name",
                 "ranking_info.character_level",
                 "ranking_info.character_exp",
                 "ranking_info.class_name",
                 "ranking_info.sub_class_name")

# fill out the empty class
df1 = df1.withColumn("class" , df1["sub_class_name"])
df1 = df1.withColumn("class" ,
                     F.when(F.col("sub_class_name") == "" , df1["class_name"]) \
                      .otherwise(F.col("class")))
df1 = df1.drop("class_name", "sub_class_name")

df2 = df2.withColumn("class" , df2["sub_class_name"])
df2 = df2.withColumn("class" ,
                     F.when(F.col("sub_class_name") == "" , df2["class_name"]) \
                      .otherwise(F.col("class")))
df2 = df2.drop("class_name", "sub_class_name")
# --------------------------------------------------------------------------------------
# 2 init2_df method
# inner join with two dataframe on character_name
df_j = df1.join(df2, 
                (df1["character_name"] == df2["character_name"]),
                "inner") 

df_j = df_j.withColumn("level_up_amount", (df2["character_level"] - df1["character_level"]))

df_j = df_j.select(df2["character_name"],
                   df1["character_level"],
                   df1["character_exp"],
                   df2["character_level"],
                   df2["character_exp"],
                   df2["class"],
                   "level_up_amount") \
           .orderBy(F.desc("level_up_amount"))

#df_j.show(10,False) # reuse this dataframe #1
# --------------------------------------------------------------------------------------
# model 1) increase_exp amount compare with 1 day before
# create "need_exp" with df_e
df_j_1 = df_j.join(df_e,
                 (df1["character_level"] == df_e["level"]), 
                 "inner")   # to calculate increase_exp
                            # use yesterday character_level

# create "increase_exp" with some condition
df_m_1 = df_j_1.withColumn("increase_exp" ,
                          F.when(df1["character_level"] != df2["character_level"] ,
                            ((df_j_1["need_exp"] - df1["character_exp"]) + df2["character_exp"]))      
                           .when(df2["character_level"] == df1["character_level"] ,
                              (df2["character_exp"] - df1["character_exp"])) \
                           .otherwise(None)
                        ) 

# reuse df_m_1 for datamodel "3"
df_m_1 = df_m_1.select("character_name",
                   "class",
                   df2["character_level"],
                   #"level_up_amount",
                   "increase_exp",
                   ) \
                .orderBy(F.desc("increase_exp")) 

#df_m_1.show(10,False) # need weekly plan #2
# --------------------------------------------------------------------------------------
# model 2) predict day for level_up

df_j_2 = df_j.join(df_e,
                   (df2["character_level"] == df_e["level"]),
                    "inner")  # to calculate predict day
                              # use today character_level

# create "need_exp_level_up"
df_j_2 = df_j_2.withColumn("need_exp_level_up",
                           F.when(df2["character_level"] != 0 ,
                                  (df_j_2["need_exp"] - df2["character_exp"])))

df_j_2 = df_j_2.select("character_name",
                       df2["character_level"],
                       "class",
                       "need_exp_level_up",
                       "level_up_amount"
                       )

# predict level up date with yesterday increase_exp amount!(daily plan)

# join with name_key (increase_exp and need_exp)
df_m_2 = df_m_1.join(df_j_2,
                   (df_m_1["character_name"] == df_j_2["character_name"]),
                   "inner")   # who character_name change is except


# create "predict_date"
# if increase_exp == 0 then replace with "1"
df_m_2 = df_m_2.select(df_m_1["character_name"],
                   df_m_1["class"],
                   df_m_1["character_level"], # today character_level
                                              # use alias with join table...
                   "need_exp_level_up",
                   "increase_exp",
                   "level_up_amount"
                   )

df_m_2 = df_m_2.withColumn("need_day_level_up",
                        F.when(F.col("increase_exp") == 0, 1)
                        .otherwise(F.round(df_m_2["need_exp_level_up"] / df_m_2["increase_exp"]))
                        )
                       

df_m_2 = df_m_2.select("*") \
               .orderBy(F.desc("character_level"),
                        F.asc("need_day_level_up"))


#df_m_2.show(10,False)   # 3
# --------------------------------------------------------------------------------------
# model 3) status 별 class의 increase_exp ratio (격차 확인)
#          max_increase class?
#          status 별 구체적인 평균 플레이타임시간에 대한 경험치 상승량 

mean_exp = 500000000000
df_m_3 = location2_df(df_m_1)
df_m_3 = df_m_3.select("*") \
               .where(F.col("increase_exp") >= mean_exp) \
               .orderBy(F.asc("increase_exp"))
                  #filter(F.col("increase_exp") != 0)

# set a key_value
df_m_3 = df_m_3.withColumn("group_key",
                           (
                              F.concat(
                                 F.col("class"),
                                 F.lit("-"),
                                 F.col("status")
                              )
                           )
)

# count column
df_m_3_a = df_m_3.select("*") \
                 .groupBy("group_key","status","class") \
                 .count()
               
# sum ,max and avg column
df_m_3_b = df_m_3.select("*") \
                 .groupBy("group_key","status","class") \
                 .agg(F.sum("increase_exp").alias("increase_exp_sum"), \
                      F.max("increase_exp").alias("increase_exp_max"), \
                      F.round(F.avg("increase_exp")).alias("increase_exp_avg"))
                  
# join data
df_m_3 = df_m_3_a.join(df_m_3_b ,
                       df_m_3_a["group_key"] == df_m_3_b["group_key"],
                       "inner")

df_m_3 = df_m_3.select(df_m_3_a["group_key"],
                       df_m_3_a["class"],
                       df_m_3_a["status"],
                       "count",
                       "increase_exp_sum",
                       "increase_exp_max",
                       "increase_exp_avg")

hunt_rank = Window.partitionBy("status").orderBy(F.desc("increase_exp_avg"))
df_m_3 = df_m_3.withColumn("hunting_rank"
                           , F.rank().over(hunt_rank)
                           )
df_m_3.show(10,False)

df_m_3.select("*") \
      .where(F.col("status") == "Tallahart") \
      .show(10,False)
#--------------------------------------------------------------------------------------







