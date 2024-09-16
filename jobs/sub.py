# time series analysis with elasticsearch
# needs for some date range data

from pyspark.sql import SparkSession , Window
import pyspark.sql.functions as F
from pyspark.sql.functions import when

import sys
import argparse

from mses import Es , Ms
from base import *
from filter import *
from datetime import *


paser = argparse.ArgumentParser()
args = paser.parse_args()

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .master("local")
             .appName("spark-nexon_api_es")
             .config("spark.jars.packages" , 'mysql:mysql-connector-j-9.0.0.jar')
             .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .config("spark.jars", "opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .getOrCreate())
    args.spark = spark

    # load exp data
    args.input_path1 = "/opt/bitnami/spark/data/maple_exp.csv"

    # input year and month 
    target_year_month = "2024-08"

    # target_date : today data (timestamp)
    # target_date1 : yesterday data

    # put date for needs
    for i in range(2,32):
        if i == 1:
            print("1일 데이터는 main파일을 이용해 주세요")
        elif 2 <= i <= 9:
            args.target_date = f"{target_year_month}-0{i}"
            args.target_date1 = f"{target_year_month}-0{i-1}"
        elif i == 10:
            args.target_date = f"{target_year_month}-{i}"
            args.target_date1 = f"{target_year_month}-0{i-1}"
        else :
            args.target_date = f"{target_year_month}-{i}"
            args.target_date1 = f"{target_year_month}-{i-1}"

        args.input_path2 = f"/opt/bitnami/spark/data/ranking_{args.target_date}.json"
        args.input_path3 = f"/opt/bitnami/spark/data/ranking_{args.target_date1}.json"

        # load data
        df_e = read_input_csv(args.spark , args.input_path1)
        df_t = read_input(args.spark, args.input_path2)
        df_y = read_input2(args.spark, args.input_path3)

        # load and preprocessed data (for daily status)
        df = init_df(df_t)
        df_a = init_df_a(df_t)

        # load and preporcessed data (for comparing)
        df_e = init_df_e(df_e)
        df_t = init_df(df_t)
        df_y = init_df(df_y)
        df2 = init2_df(df_t, df_y)

        # ---------------------------------------------------------|
        dist_filter = TopClassFilter(args)
        dist_df = location_df(df)
        dist_df = dist_filter.filter(dist_df)   # -- datamodel 1
        # ---------------------------------------------------------|
        exp_user = TopExpUserFilter(args)
        expuser_df = exp_user.filter(df_e, df_y, df_t, df2)
        # top Hunting class filter (with df2)
        mean_exp = 500000000000
        exp_class = TopHuntingClassFilter(args)
        df_1 = location2_df(expuser_df)
        tophuntclass_df = exp_class.filter(df_1, mean_exp)    # -- datamodel 2
        
        # ---------------------------------------------------------|
        # predict day filter (with df2)
        predict_day = PredictDayFilter(args)
        predict_day_df = predict_day.filter(df_e, df2, df_t, expuser_df) # -- datamodel 3 (personal trace)
        
        # ---------------------------------------------------------|
        # Status_Change_count filter (with df2)
        status_change = StatusChangeCount(args)
        status_change_df = status_change.filter(df2, df_y, df_t)    # -- datamodel 5 (status_change_info)


        ## save four data model to elasticSearch
        es = Es("http://es:9200")
        #es.write_elasticesearch(dist_df, f"ranking_{args.target_date}") # 1
        #es.write_elasticesearch(tophuntclass_df, f"hunting_data_{args.target_date}") # 2
        #es.write_elasticesearch(predict_day_df , f"personal_exp_{args.target_date}") # 3
        #es.write_elasticesearch(status_change_df, f"status_change_{args.target_date}") # 5


        ## save data model to MySQL
        ms = Ms("jdbc:mysql://172.21.80.1:3306/MapleRanking")
        #ms.write_to_mysql(dist_df,"distribution")
        #ms.write_to_mysql(status_change_df, "status")
        #ms.write_to_mysql(tophuntclass_df, "hunting")

        ms = Ms("jdbc:mysql://172.21.80.1:3306/PersonalTrace")
        classtrace = ClassTraceFilter(args)
        classtrace_df = classtrace.filter(df_a)     
        classtrace_df.show(20,False)        
        ms.write_to_mysql(classtrace_df,"class")
        #ms.write_to_mysql(predict_day_df, "levelup")

        #ms.write_to_mysql(classtrace_df,"study_SQL")
        
