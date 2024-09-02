# Extract ranking data // transform with spark session // loaded data at RDBMS(MySQL)
# daily session


import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import *
from filter import TopClassFilter, TopHuntingClassFilter ,TopExpUserFilter, PredictDayFilter
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

    # today ranking data
    args.target_date = datetime.now().strftime("2024-%m-%d")
    args.input_path2 = f"/opt/bitnami/spark/data/ranking_{args.target_date}.json"
    #args.input_path2 = "/opt/bitnami/spark/data/ranking_2024-08-30.json"
    

    # yesterday ranking data
    args.target_date1 = (datetime.now() - timedelta(1)).strftime("2024-%m-%d")
    args.input_path3 = f"/opt/bitnami/spark/data/ranking_{args.target_date1}.json"
    #args.input_path3 = f"/opt/bitnami/spark/data/ranking_2024-08-29.json"
    

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
    df = location2_df(expuser_df)
    tophuntclass_df = exp_class.filter(df, mean_exp)    # -- data model2

    # predict day filter (with df2)
    predict_day = PredictDayFilter(args)
    predict_day_df = predict_day.filter(df_e, df2, df_t, expuser_df)    # -- data model3

    # show spark dataframe
    dist_df.show(10, False)
    tophuntclass_df.show(10,False)

    expuser_df.show(10,False)
    predict_day_df.show(10,False)
    

    # save to MySQL RDBMS
    DB_NAME1 = "MapleRanking"
    DB_NAME2 = "PersonalTrace"

    #daily session!
    mysql1 = Ms(f"jdbc:mysql://172.21.80.1:3306/{DB_NAME1}")
    mysql1.write_to_mysql(dist_df, "level_distribution")
    mysql1.write_to_mysql(tophuntclass_df, "top_increase_exp_class")
    
    
    # daily session (need fix to append..)
    mysql2 = Ms(f"jdbc:mysql://172.21.80.1:3306/{DB_NAME2}")
    mysql2.write_to_mysql(predict_day_df, "predict_day_levelup")  
    
    