from base import BaseFilter
import pyspark.sql.functions as F
from pyspark.sql import Window


# daily filter
class TopClassFilter(BaseFilter):
    def filter(self, df):
        class_df = df.groupBy("class","date") \
                     .pivot("status") \
                     .count()
        
        map_list = ["Tallahart", "Carcion", "Arteria", "Dowonkyung"]
        class_df = class_df.withColumn("sum"
                                         ,sum(F.col(c) for c in map_list))

        window = Window.orderBy(F.desc("sum"))
        class_df = class_df.withColumn("rank", F.rank().over(window))
        # set a key value - elastic search
        class_df = class_df.withColumn("key_value",
                                       (
                                           F.concat(
                                               F.col("date"),
                                               F.lit("-"),
                                               F.col("class")
                                           )
                                       ))
        
        class_df = class_df.select(["key_value",
                                    "Tallahart","Carcion","Arteria","Dowonkyung",
                                    "sum","rank","date","class"])
        
        return class_df

# analysis increase ratio of class exp
# another main.py for weekly plan)
class TopHuntingClassFilter(BaseFilter):
    # df : depend on init2_df -> TopexpUserMethod -> location2_df
    # mean_exp : get_exp with mean play time
    def filter(self, df, mean_exp):
        df = df.select("*") \
               .where(F.col("increase_exp") >= mean_exp) \
               .orderBy(F.asc("increase_exp"))
        
        df = df.withColumn("group_key",
                           (
                               F.concat(
                                   F.col("class"),
                                   F.lit("-"),
                                   F.col("status")
                               )
                           ))
        # data to join
        df_a = df.select("*") \
                 .groupBy("group_key","status","class","date") \
                 .count()
        
        df_b = df.select("*") \
                 .groupBy("group_key", "status","class","date") \
                 .agg(F.sum("increase_exp").alias("increase_exp_sum"), \
                      F.max("increase_exp").alias("increase_exp_max"),
                      F.avg("increase_Exp").alias("increase_exp_avg"))
        # join data
        df = df_a.join(df_b,
                       df_a["group_key"] == df_b["group_key"],
                       "inner")
    
        
        df = df.select(df_a["group_key"],
                       df_a["class"],
                       df_a["status"],
                       "count",
                       "increase_exp_sum",
                       "increase_exp_max",
                       "increase_exp_avg",
                       df_a["date"]
                       )
        
        hunt_rank = Window.partitionBy("status").orderBy(F.desc("increase_exp_avg"))
        
        df = df.withColumn("hunting_rank"
                           , F.rank().over(hunt_rank)
                           )
        

        return df
    

# detect exp history (compare with yesterday data) - sub method
# depend on init2_df method
class TopExpUserFilter(BaseFilter):
    # df1 = exp_data, df2 = yesterday_data, df3 = today_data, df4 = init2_df return
    def filter(self, df1, df2, df3, df4):
        df_j_1 = df4.join(df1,
                     (df2["character_level"] == df1["level"]),  
                     "inner")
        df = df_j_1.withColumn("increase_exp",
                           F.when(df2["character_level"] != df3["character_level"],
                                  ((df_j_1["need_exp"] - df2["character_exp"]) + df3["character_exp"]))
                            .when(df3["character_level"] == df2["character_level"],
                                  (df3["character_exp"] - df2["character_exp"])) \
                            .otherwise(None)
                            )

        df = df.select("character_name",
                       "class",
                       df3["character_level"],
                       "increase_exp",
                       "level_up_amount",
                       df4["date"]
                       )\
                .orderBy(F.desc("increase_exp"))
        
        exp_rank = Window.orderBy(F.desc("increase_exp"))
        
        df = df.withColumn("exp_rank"
                           , F.rank().over(exp_rank)
                           )
        return df

# detect predict day(predict leave user)
# depend on init2_df method
class PredictDayFilter(BaseFilter):
    # df1 = exp_data, df2 = init2_df return, df3 = today_data, df4 = TopExpUserFilter return dataframe
    # df4 columns : c_name, class, c_level(today), increase_exp, date(today)
    def filter(self, df1, df2, df3, df4):
        df_f = df2.join(df1,
                      (df3["character_level"] == df1["level"]),
                      "inner")
        df_f = df_f.withColumn("need_exp_level_up",
                           F.when(df3["character_level"] != 0,
                                  ((df_f["need_exp"]) - (df3["character_exp"]))
                                  ))
        df_f = df_f.select("character_name",
                       df3["character_level"],
                       "class",
                       "level_up_amount",
                       "need_exp_level_up"
                       )
        df = df4.join(df_f,
                      (df4["character_name"] == df_f["character_name"]),
                      "inner"
                      )
        #df.show(10,False)
        df = df.select(df4["character_name"],
                       df4["class"],
                       df4["character_level"],
                       "need_exp_level_up",
                       "increase_exp",
                       df4["date"]
                       )
        
        df = df.withColumn("need_day_level_up",
                           F.when(F.col("increase_exp") == 0 ,"we need you T^T")
                            .when(((F.col("increase_exp")) >= (df["need_exp_level_up"])) , "Congratulation!")
                            .otherwise(F.round(df["need_exp_level_up"] / df["increase_exp"]))
                            )
        df = df.select("*") \
               .orderBy(F.desc("character_level"),
                        F.asc("need_day_level_up"))
        return df
    
# depand on init_df2 method
class StatusChangeCount(BaseFilter):
    # df = init_df2 return , df1 = yesterday_data, df2 = today_data
    def filter(self, df,  df1, df2):
        df = df.withColumn("status_change",
                   F.when((df1["character_level"] == 279) & (df2["character_level"] == 280) , "go Arteria!") \
                        .when((df1["character_level"] == 284) & (df2["character_level"] == 285) , "go Carcion!") \
                        .when((df1["character_level"] == 289) & (df2["character_level"] == 290) , "go Tallahart!")
                        .otherwise("stay here"))

        df = df.select("class",
               "date",
               df2["character_level"],
               "status_change")
        df = df.select("*") \
               .groupBy("class",
                "date",
                "status_change")    \
               .count()
        return df
