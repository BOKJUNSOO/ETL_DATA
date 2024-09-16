# Extract ranking data // transform with spark session // loaded data at RDBMS(MySQL)
# daily session


import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import *
from filter import TopClassFilter, TopHuntingClassFilter ,TopExpUserFilter, PredictDayFilter, StatusChangeCount, ClassTraceFilter
from mses import Ms , Es
# four data model that i wanted
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = (SparkSession
             .builder
             .master("local")
             .appName("spark-nexon_api_ms")
             .config("spark.jars.packages" , 'mysql:mysql-connector-j-9.0.0.jar')
             .getOrCreate())
    args.spark = spark

    # load exp data
    args.input_path1 = "/opt/bitnami/spark/data/maple_exp.csv"

    # today ranking data @timestamp
    args.target_date = datetime.now().strftime("2024-%m-%d")
    #args.target_date = "2024-09-01"
    args.input_path2 = f"/opt/bitnami/spark/data/ranking_{args.target_date}.json"
    
    
    # yesterday ranking data
    args.target_date1 = (datetime.now() - timedelta(1)).strftime("2024-%m-%d")
    #args.target_date1 = "2024-08-31"
    args.input_path3 = f"/opt/bitnami/spark/data/ranking_{args.target_date1}.json"


    df_e = read_input_csv(args.spark, args.input_path1)  #exp data
    df_t = read_input(args.spark, args.input_path2)  #today data
    df_y = read_input2(args.spark , args.input_path3)   #yesterday data

    # load and preprocessed data (for daily status)
    df = init_df(df_t)

    # load and preporcessed data (for comparing)
    df_e = init_df_e(df_e)
    df_t = init_df(df_t)
    df_y = init_df(df_y)
    df2 = init2_df(df_t, df_y)

    # DataModel
    # daily status filter - location amount & Top rate class
    dist_filter = TopClassFilter(args)
    dist_df = location_df(df)
    dist_df = dist_filter.filter(dist_df)       # -- data model1

    # top exp user filter (with exp_data, yesterday_data, today_data, init2_df)
    exp_user = TopExpUserFilter(args)
    expuser_df = exp_user.filter(df_e, df_y, df_t, df2)

    # top Hunting class filter (with df2)
    mean_exp = 500000000000
    exp_class = TopHuntingClassFilter(args)
    df_1 = location2_df(expuser_df)
    tophuntclass_df = exp_class.filter(df_1, mean_exp)    # -- data model 2

    # predict day filter (with df2)
    predict_day = PredictDayFilter(args)
    predict_day_df = predict_day.filter(df_e, df2, df_t, expuser_df)    # -- data model 3

    # class trace filter (with today data) -- personal Trace
    classtrace = ClassTraceFilter(args)
    classtrace_df = classtrace.filter(df)     # -- data model 4

    # StatusChangeCount (with df2)
    status_change = StatusChangeCount(args)
    status_change_df = status_change.filter(df2, df_y, df_t)    # -- data model 5

    # show spark dataframe
    
    # spark dataframe to write
    #dist_df.show(10, False) 
    #tophuntclass_df.show(10,False)
    #predict_day_df.select("*") \
    #              .orderBy(F.asc("need_exp_level_up")) \
    #              .show(10,False)
    #classtrace_df.show(10,False)
    #status_change_df.show(30,False)

    # save to MySQL RDBMS
    DB_NAME1 = "MapleRanking"
    DB_NAME2 = "PersonalTrace"

    #daily session_ mapleranking schema
    mysql1 = Ms(f"jdbc:mysql://172.21.80.1:3306/{DB_NAME1}")
    #mysql1.write_to_mysql(dist_df, "distribution")
    #mysql1.write_to_mysql(tophuntclass_df, "hunting")
    #mysql1.write_to_mysql(status_change_df, "status")
    
    
    # daily session _personal trace schema
    mysql2 = Ms(f"jdbc:mysql://172.21.80.1:3306/{DB_NAME2}")
    #mysql2.write_to_mysql(predict_day_df, "levelup")  
    mysql2.write_to_mysql(classtrace_df, "class")
    

