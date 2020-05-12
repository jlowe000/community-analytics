import configparser
import transform_load as tl
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *

config = configparser.RawConfigParser()
config.read('database.properties')

url = config.get('postgres-slack','database.url')
driver = config.get('postgres-slack','database.driver')
user = config.get('postgres-slack','database.user')
password = config.get('postgres-slack','database.password')
jars = config.get('postgres-slack','spark.jars')

spark = SparkSession.builder.master('local').appName('SlackBot').config('spark.jars',jars).getOrCreate();

def load_channel():
  data_name = 'channel_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('id', StringType(), True)
                       ,StructField('name', StringType(), True)
                       ,StructField('type', StringType(), True)
                       ,StructField('class', StringType(), True)
                       ,StructField('is_archived', StringType(), True)
                       ,StructField('is_private', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_user():
  data_name = 'user_data'
  dataset = 'master/csv'
  dd_schema = StructType([ StructField('id', StringType(), True)
                          ,StructField('name', StringType(), True)
                          ,StructField('real_name', StringType(), True)
                          ,StructField('tz', StringType(), True)
                          ,StructField('email', StringType(), True)
                          ,StructField('batch', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,dd_schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_reaction():
  data_name = 'reaction_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('channel', StringType(), True)
                       ,StructField('ts', StringType(), True)
                       ,StructField('thread_ts', StringType(), True)
                       ,StructField('user', StringType(), True)
                       ,StructField('reaction', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_poll():
  data_name = 'poll_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('poll_id', StringType(), True)
                       ,StructField('ts', StringType(), True)
                       ,StructField('time', StringType(), True)
                       ,StructField('text', StringType(), True)
                       ,StructField('vote_item', StringType(), True)
                       ,StructField('vote_count', IntegerType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df = df.withColumnRenamed("time","time_as_str")
  df = df.withColumn("time",to_timestamp(col("time_as_str")))
  df = df.drop("time_as_str")
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_file():
  data_name = 'file_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('id', StringType(), True)
                       ,StructField('channel', StringType(), True)
                       ,StructField('name', StringType(), True)
                       ,StructField('time', StringType(), True)
                       ,StructField('user', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df = df.withColumnRenamed("time","time_as_str")
  df = df.withColumn("time",to_timestamp(col("time_as_str")))
  df = df.drop("time_as_str")
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_conversation():
  data_name = 'conversation_data'
  dataset = 'metrics/csv'
  schema = StructType([ StructField('channel', StringType(), True)
                       ,StructField('type', StringType(), True)
                       ,StructField('subtype', StringType(), True)
                       ,StructField('ts', StringType(), True)
                       ,StructField('thread_ts', StringType(), True)
                       ,StructField('ts_int', DoubleType(), True)
                       ,StructField('time', StringType(), True)
                       ,StructField('user', StringType(), True)
                       ,StructField('real_name', StringType(), True)
                       ,StructField('name', StringType(), True)
                       ,StructField('text', StringType(), True)
                       ,StructField('city', StringType(), True)
                       ,StructField('country', StringType(), True)
                       ,StructField('iso', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df = df.withColumnRenamed("time","time_as_str")
  df = df.withColumn("time",to_timestamp(col("time_as_str")))
  df = df.drop("time_as_str")
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_tag():
  data_name = 'tag_data'
  dataset = 'metrics/csv'
  schema = StructType([ StructField('channel', StringType(), True)
                       ,StructField('ts', StringType(), True)
                       ,StructField('tag', StringType(), True)
                       ,StructField('nice_tag', StringType(), True)
                       ,StructField('type', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_node():
  data_name = 'node_data'
  dataset = 'metrics/csv'
  schema = StructType([ StructField('id', StringType(), True)
                       ,StructField('label', StringType(), True)
                       ,StructField('type', StringType(), True)
                       ,StructField('name', StringType(), True)
                       ,StructField('iso', StringType(), True)
                       ,StructField('country', StringType(), True)
                       ,StructField('city', StringType(), True)
                       ,StructField('channel_type', StringType(), True)
                       ,StructField('channel_class', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_edge():
  data_name = 'edge_data'
  dataset = 'metrics/csv'
  schema = StructType([ StructField('channel', StringType(), True)
                       ,StructField('source', StringType(), True)
                       ,StructField('target', StringType(), True)
                       ,StructField('relate', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_master_data():
  load_channel();
  load_reaction();
  load_user();
  load_file();
  load_poll();

def load_metric_data():
  load_conversation();
  load_tag();
  load_node();
  load_edge();

if __name__ == '__main__':
  print('loading master data')
  load_master_data()
  print('loading metric data')
  load_metric_data()
  print('done')
