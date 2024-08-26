import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import *
from filter import TopClassFilter, TopHuntingClassFilter ,TopExpUserFilter, PredictDayFilter
from ms import Ms
# four data model that i wanted
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = (SparkSession
             .builder
             .master("local")
             .appName("spark-nexon_api")
             .config("spark.jars.packages" , 'mysql:mysql-connector-j-9.0.0.0.jar')
             #.config("spark.jars", "/opt/bitnami/spark/resources/mysql-connector-j-9.0.0.jar")
             .getOrCreate())
    args.spark = spark

    # load exp data
    args.input_path1 = "/opt/bitnami/spark/data/maple_exp.csv"

    # today ranking data
    args.target_date = datetime.now().strftime("2024-%m-%d")
    args.input_path2 = f"/opt/bitnami/spark/data/ranking_{args.target_date}.json"

    # yesterday ranking data
    args.target_date1 = (datetime.now() - timedelta(1)).strftime("2024-%m-%d")
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
    dist_df = dist_filter.filter(dist_df)

    # top exp user filter (with exp_data, yesterday_data, today_data, init2_df)
    exp_user = TopExpUserFilter(args)
    expuser_df = exp_user.filter(df_e, df_y, df_t, df2)

    # predict day filter (with df2)
    predict_day = PredictDayFilter(args)
    predict_day_df = predict_day.filter(df_e, df2, df_t, expuser_df)

    # top Hunting class filter (with df2)
    mean_exp = 500000000000
    exp_class = TopHuntingClassFilter(args)
    df = location2_df(expuser_df)
    tophuntclass_df = exp_class.filter(df, mean_exp)

    # show spark dataframe
    dist_df.show(10, False)
    expuser_df.show(10,False)
    predict_day_df.show(10,False)
    tophuntclass_df.show(10,False)

    # save to MySQL RDBMS
    mysql = Ms("jdbc:mysql://172.21.80.1:3306/MapleRanking")
    mysql.write_to_mysql(dist_df, "level_distribution")
    mysql.write_to_mysql(expuser_df , "top_increase_exp_user")
    mysql.write_to_mysql(predict_day_df, "predict_day_levelup")
    mysql.write_to_mysql(tophuntclass_df, "top_increase_exp_class")