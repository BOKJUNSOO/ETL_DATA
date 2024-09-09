# save at RDBMS (MySQL)
class Ms(object):
    def __init__(self,ms_host):
        self.ms_host = ms_host

    def write_to_mysql(self, df, table_name):
        df.write    \
          .mode('append') \
          .format("jdbc") \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("url", self.ms_host ) \
          .option("dbtable", table_name ) \
          .option("user", "root") \
          .option("password", "mysql") \
          .save()

# save at ElasticSearch (for kibana)
class Es(object):
  def __init__(self, es_hosts, mode="append", write_operation="overwrite"):
    self.es_hosts = es_hosts
    self.es_mode = mode
    self.es_write_operation = write_operation
    self.es_index_auto_create = "yes"

  def write_elasticesearch(self, df, es_resource):
    df.write    \
      .mode(self.es_mode) \
      .format("org.elasticsearch.spark.sql") \
      .option("es.nodes", self.es_hosts) \
      .option("es.index.auto.create", self.es_index_auto_create) \
      .option("es.resource", es_resource) \
      .save()