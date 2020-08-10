import configparser
import transform_load as tl
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

config = configparser.RawConfigParser()
config.read('database.properties')

url = config.get('oracle-slack','database.url')
driver = config.get('oracle-slack','database.driver')
user = config.get('oracle-slack','database.user')
password = config.get('oracle-slack','database.password')
jars = config.get('oracle-slack','spark.jars')

print(url)
print(driver)
print(user)
print(jars)

spark = SparkSession.builder.master('local').appName('SlackBot').config('spark.jars',jars).getOrCreate();

def alter_column(df,data_name,col):
  df2 = df.withColumn(col+'_len', length(encode(df[col],'utf-8')))
  max_value = df2.select(max(col+'_len')).collect()[0][0]
  print(max_value)
  dbdf = spark.read.jdbc(url, data_name, properties={'user': user, 'password': password, 'driver': driver})
  dbdf2 = dbdf.withColumn(col+'_len', length(dbdf[col]))
  db_max_value = dbdf2.select(max(col+'_len')).collect()[0][0]
  print(db_max_value)
  if db_max_value != None and max_value < db_max_value:
    return col + ' VARCHAR('+ str(db_max_value) +')'
  else:
    return col + ' VARCHAR('+ str(max_value) +')'

def load_channel():
  data_name = 'channel_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('index', IntegerType(), True)
                       ,StructField('id', StringType(), True)
                       ,StructField('name', StringType(), True)
                       ,StructField('type', StringType(), True)
                       ,StructField('class', StringType(), True)
                       ,StructField('is_archived', StringType(), True)
                       ,StructField('is_private', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'name')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_channel_ref():
  data_name = 'channel_ref'
  dataset = 'references'
  schema = StructType([ StructField('id', StringType(), True)
                       ,StructField('index', IntegerType(), True)
                       ,StructField('category_name', StringType(), True)
                       ,StructField('category_index', IntegerType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  df.write.jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_user():
  data_name = 'user_data'
  dataset = 'master/csv'
  dd_schema = StructType([ StructField('index', IntegerType(), True)
                          ,StructField('id', StringType(), True)
                          ,StructField('name', StringType(), True)
                          ,StructField('real_name', StringType(), True)
                          ,StructField('tz', StringType(), True)
                          ,StructField('email', StringType(), True)
                          ,StructField('batch', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,dd_schema)
  df.show()
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'email')
  col_types = col_types + ', ' + alter_column(df,table_name,'name')
  col_types = col_types + ', ' + alter_column(df,table_name,'real_name')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_reaction():
  data_name = 'reaction_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('index', IntegerType(), True)
                       ,StructField('channel', StringType(), True)
                       ,StructField('ts', StringType(), True)
                       ,StructField('thread_ts', StringType(), True)
                       ,StructField('user_id', StringType(), True)
                       ,StructField('reaction', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df.show()
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'reaction')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_poll():
  data_name = 'poll_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('index', IntegerType(), True)
                       ,StructField('poll_id', StringType(), True)
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
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'text')
  col_types = col_types + ', ' + alter_column(df,table_name,'vote_item')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

def load_file():
  data_name = 'file_data'
  dataset = 'master/csv'
  schema = StructType([ StructField('index', IntegerType(), True)
                       ,StructField('id', StringType(), True)
                       ,StructField('channel', StringType(), True)
                       ,StructField('name', StringType(), True)
                       ,StructField('time', StringType(), True)
                       ,StructField('user_id', StringType(), True) ])
  dd = tl.dd_readfile(dataset,data_name)
  df = spark.createDataFrame(dd,schema)
  df = df.withColumnRenamed("time","time_as_str")
  df = df.withColumn("time",to_timestamp(col("time_as_str")))
  df = df.drop("time_as_str")
  df.show()
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'name')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

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
                       ,StructField('user_id', StringType(), True)
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
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'real_name')
  col_types = col_types + ', ' + alter_column(df,table_name,'text')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, data_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

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
  table_name = data_name.upper()
  col_types = alter_column(df,table_name,'tag')
  col_types = col_types + ', ' + alter_column(df,table_name,'nice_tag')
  df.write.option("createTableColumnTypes", col_types).jdbc(url, table_name, mode='overwrite', properties={'user': user, 'password': password, 'driver': driver})

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
  print('loading master data')
  load_channel();
  print('loading master data')
  load_channel_ref();
  print('loading master data')
  load_reaction();
  print('loading master data')
  load_user();
  print('loading master data')
  load_file();
  print('loading master data')
  load_poll();

def load_metric_data():
  print('loading metric data')
  load_conversation();
  print('loading metric data')
  load_tag();
  print('loading metric data')
  load_node();
  print('loading metric data')
  load_edge();

if __name__ == '__main__':
  print('loading master data')
  load_master_data()
  print('loading metric data')
  load_metric_data()
  print('done')
