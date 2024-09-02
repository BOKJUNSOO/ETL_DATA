# time series analysis with elasticsearch
# needs for some date range data (data model 2,3)

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
             .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .config("spark.jars", "opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .getOrCreate())
    args.spark = spark

    # load exp data
    args.input_path1 = "/opt/bitnami/spark/data/maple_exp.csv"
    
    # put date for needs
    for i in range(11,26):
        # today data
        args.target_date = f"2024-08-{i}"
        args.input_path2 = f"/opt/bitnami/spark/data/ranking_{args.target_date}.json"

        # one day before data
        args.target_date1 = f"2024-08-{i-1}"
        args.input_path3 = f"/opt/bitnami/spark/data/ranking_{args.target_date1}.json"

        # load data
        df_e = read_input_csv(args.spark , args.input_path1)
        df_t = read_input(args.spark, args.input_path2)
        df_y = read_input2(args.spark, args.input_path3)

            # load and preprocessed data (for daily status)
        df = init_df(df_t)

        # load and preporcessed data (for comparing)
        df_e = init_df_e(df_e)
        df_t = init_df(df_t)
        df_y = init_df(df_y)
        df2 = init2_df(df_t, df_y)

        #-- DataModel_1
        # daily status filter - location amount & Top rate class
        # can use this order with merged data
        #dist_filter = TopClassFilter(args)
        #dist_df = location_df(df)
        #dist_df = dist_filter.filter(dist_df)   # -- datamodel 1
        # ---------------------------------------------------------|
        #-- DataModel_2
        # use another order with merged data for this datamodel
        exp_user = TopExpUserFilter(args)
        expuser_df = exp_user.filter(df_e, df_y, df_t, df2)

        # top Hunting class filter (with df2)
        mean_exp = 500000000000
        exp_class = TopHuntingClassFilter(args)
        df = location2_df(expuser_df)
        tophuntclass_df = exp_class.filter(df, mean_exp)    # -- datamodel 2
        # ---------------------------------------------------------|
        # predict day filter (with df2)
        predict_day = PredictDayFilter(args)
        predict_day_df = predict_day.filter(df_e, df2, df_t, expuser_df) # -- datamodel 3 (personal trace)
        
        # save three data model to elasticSearch
        es = Es("http://es:9200")
        es.write_elasticesearch(tophuntclass_df, f"hunting_data_{args.target_date}")
        es.write_elasticesearch(predict_day_df , f"personal_data_{args.target_date}")
