import glob

from abc import ABC, abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.functions import when

class BaseFilter(ABC):
    def __init__(self, args):
        self.args = args
        self.spark = args.spark
        
    def filter (self,df):
        pass

# Check data exist
def read_input_csv(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read\
                  .format("csv") \
                  .option("multiLine", True) \
                  .option("header", True) \
                  .option("inferschema", True) \
                  .load(input_path) 
        df.printSchema()

        return df
    else:
        return print("exp 파일이 위치하지 않습니다.")

def read_input(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read \
                  .format("json") \
                  .option("multiLine" , True) \
                  .option("header", True) \
                  .option("inferschema", True) \
                  .load(input_path)
        df.printSchema()
        return df
    else:
        return print("금일 데이터를 먼저 수집해 주세요.")

def read_input2(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read \
                  .format("json") \
                  .option("multiLine" , True) \
                  .option("header", True) \
                  .option("inferschema", True) \
                  .load(input_path)
        df.printSchema()

        return df
    else:
        return print("전날 데이터를 먼저 수집해주세요.")
    
# Categorization level range (depend on init_df)
def location_df(df):
    df = df.withColumn("status" ,
                       when(df["character_level"] > 289, "Tallahart")
                             .when((df["character_level"] < 290) & (df["character_level"] > 284) , "Carcion")
                             .when((df["character_level"] < 285) & (df["character_level"] > 279) , "Arteria")
                             .when((df["character_level"] < 280) & (df["character_level"] > 274) , "Dowonkyung")
                             .otherwise(F.col("character_level")))
    
    # set a key value (date-class key)
    df = df.withColumn("key",
                       (F.concat(F.col("date"),
                                 F.lit("-"),
                                 F.col("class")))
                       )
    return df

# Categorization level range (depend on init_df)
# without set a key value
def location2_df(df):
    df = df.withColumn("status" ,
                       when(df["character_level"] > 289, "Tallahart")
                             .when((df["character_level"] < 290) &(df["character_level"] > 284) , "Carcion")
                             .when((df["character_level"] < 285) & (df["character_level"] > 279) , "Arteria")
                             .when((df["character_level"] < 280) & (df["character_level"] > 274) , "Dowonkyung")
                             .otherwise(F.col("character_level")))
    return df

# dataframe for ADVENTURERS
def init_df_a(df):
    # select colum to use
    adventure_list = ["전사", "도적", "궁수", "마법사", "해적"]

    df = df.select(F.explode("ranking")
                   .alias("ranking_info"))
    df = df.select("ranking_info.date",
                   "ranking_info.character_name",
                   "ranking_info.character_level",
                   "ranking_info.class_name",
                   "ranking_info.sub_class_name")
    
    df = df.filter(df["class_name"]
              .isin(adventure_list))
    return df
    

# Preprocessing for data store
def init_df(df):
    # select column to use
    df = df.select(F.explode("ranking")
                         .alias("ranking_info"))
    df = df.select("ranking_info.date",
                   "ranking_info.character_name",
                   "ranking_info.character_level",
                   "ranking_info.character_exp",
                   "ranking_info.class_name",
                   "ranking_info.sub_class_name")
    # fill class (at least not null)
    df = df.withColumn("class", df["sub_class_name"])
    df = df.withColumn("class",
                       F.when(F.col("sub_class_name") == "", df["class_name"]) \
                        .otherwise(F.col("class")))
    df = df.drop("class_name", "sub_class_name")
    return df

# join today and yesterday dataframe (depend on init_df)
def init2_df(df_t, df_y):
    df = df_y.join(df_t,
                    (df_y["character_name"] == df_t["character_name"]), # 각 인스턴스의 식별자를 닉네임으로 밖에 가져올 수 없음.
                    "inner")
    df = df.withColumn("level_up_amount",
                       (df_t["character_level"] - df_y["character_level"]))
    df = df.select(df_t["character_name"],
                   df_t["class"],
                   df_y["character_level"],
                   df_t["character_level"],
                   df_y["character_exp"],
                   df_t["character_exp"],
                   df_t["date"],
                   "level_up_amount") \
           .orderBy(F.desc("level_up_amount"))
    return df

def init_df_e(df):
    df = df.select("level",
                     "need_exp")
    return df