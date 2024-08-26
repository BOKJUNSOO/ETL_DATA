class Ms(object):
    def __init__(self,ms_host):
        self.ms_host = ms_host

    def write_to_mysql(self, df, table_name):
        df.write \
          .format("jdbc") \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("url", self.ms_host ) \
          .option("dbtable", table_name ) \
          .option("user", "root") \
          .option("password", "mysql") \
          .save()