import os
import time
import csv
import pandas
import ast
import ssl
import certifi
import glob
import sys
from math import floor
from datetime import datetime
from pyspark.sql.functions import expr
from pyspark.sql.functions import round
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

sign_token = os.environ['SLACK_SIGN_TOKEN']
access_token = os.environ['SLACK_ACCESS_TOKEN']
user_token = os.environ['SLACK_USER_TOKEN']
# user_token = 'SLACK_USER_TOKEN'  

ssl_context = ssl.create_default_context(cafile=certifi.where())
spark = SparkSession.builder.master('local').appName('SlackBot').getOrCreate();

batch = sys.argv[1]

def dd_writefile(filename,pdf):
  with open('data/'+batch+'/csv/'+filename+'.csv','a') as f:
    pdf.to_csv(f,index=False,quoting=csv.QUOTE_ALL,mode='a',header=f.tell()==0);

def sortfile_key(file):
  return int(file[-21:-5]);

def dd_comparefile(file1,file2):
  # print(file1[-21:-5]+','+file2[-21:-5],int(file1[-21:-5])<int(file2[-21:-5]));
  return int(file1[-21:-5])<=int(file2[-21:-5]);

def dd_userdata(result):
  filename = "user_data";
  try:
    schema = StructType([
               StructField('id', StringType(), True),
               StructField('name', StringType(), True),
               StructField('real_name', StringType(), True),
               StructField('tz', StringType(), True)]);
    users = result['members'];
    rdd = spark.sparkContext.parallelize(users);
    df = spark.createDataFrame(rdd,schema);
    pdf = df.toPandas();
    dd_writefile(filename,pdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_channeldata(result):
  filename = "channel_data";
  try:
    schema = StructType([
               StructField('id', StringType(), True),
               StructField('name', StringType(), True)]);
    channels = result['channels'];
    rdd = spark.sparkContext.parallelize(channels);
    df = spark.createDataFrame(rdd,schema);
    pdf = df.toPandas();
    for channel in channels:
      id = channel['id']
      retrieve_messages(id);
    dd_writefile(filename,pdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_messagedata(id,result):
  filename = "message_data";
  try:
    s1 = StructType([
               StructField('channel', StringType(), True),
               StructField('type', StringType(), True),
               StructField('subtype', StringType(), True),
               StructField('ts', StringType(), True),
               StructField('thread_ts', StringType(), True),
               StructField('time', StringType(), True),
               StructField('reply_count', IntegerType(), True),
               StructField('reactions', ArrayType(StructType([
                 StructField('name', StringType(), True),
                 StructField('users', ArrayType(StringType()), True)])), True),
               StructField('user', StringType(), True),
               StructField('text', StringType(), True)]);
    messages = result['messages']
    # print(result['messages'][0])
    r1 = spark.sparkContext.parallelize(messages);
    df1 = spark.createDataFrame(r1,s1);
    df1 = df1.na.fill({'channel':id})
    df2 = df1.withColumn('time',from_unixtime(round(col('ts').astype(FloatType())),'yyyy-MM-dd\'T\'HH:mm:ss.000'))
    # df2.show()
    pdf = df2.toPandas();
    for index, row in pdf.iterrows():
      dd_reactiondata(row)
      print('replies:'+row['ts']+','+str(row['reply_count']))
      if row['reply_count'] !=  None and row['reply_count'] > 0:
        retrieve_threads(id,row['ts']);
    pdf = pdf.drop(columns=['reactions']);
    dd_writefile(filename,pdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_threaddata(id,ts,result):
  filename = "thread_data";
  try:
    s1 = StructType([
               StructField('channel', StringType(), True),
               StructField('type', StringType(), True),
               StructField('subtype', StringType(), True),
               StructField('ts', StringType(), True),
               StructField('thread_ts', StringType(), True),
               StructField('time', StringType(), True),
               StructField('user', StringType(), True),
               StructField('text', StringType(), True)]);
    messages = result['messages']
    r1 = spark.sparkContext.parallelize(messages);
    df1 = spark.createDataFrame(r1,s1);
    df1 = df1.na.fill({'channel':id})
    df2 = df1.withColumn('time',from_unixtime(round(col('ts').astype(FloatType())),'yyyy-MM-dd\'T\'HH:mm:ss.000'))
    # df2.show()
    pdf = df2.toPandas();
    for index, row in pdf.iterrows():
      dd_reactiondata(row)
    dd_writefile(filename,pdf)
    print('end');
  except Exception as err:
    print('Error')
    print(err)

def dd_reactiondata(row):
  filename = "reaction_data";
  try:
    schema = StructType([
               StructField('reaction', StringType(), True),
               StructField('ts', StringType(), True),
               StructField('user', StringType(), True)]);
    # print(row)
    reactions = row.get('reactions',None);
    if (reactions != None):
      l = []
      for reaction in reactions:
        for user in reaction[1]:
          l.append({'user': user, 'ts': str(row['ts']), 'reaction':str(reaction[0])});
      rdd = spark.sparkContext.parallelize(l);
      df = spark.createDataFrame(rdd,schema);
      # print(df.schema);
      # df.show();
      pdf = df.toPandas();
      dd_writefile(filename,pdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_filedata(result):
  filename = "file_data";
  try:
    schema = StructType([
               StructField('id', StringType(), True),
               StructField('channels', ArrayType(StringType()), True),
               StructField('channel', StringType(), True),
               StructField('name', StringType(), True),
               StructField('timestamp', StringType(), True),
               StructField('time', StringType(), True),
               StructField('user', StringType(), True)]);
    sc = StructType([
               StructField('id', StringType(), True),
               StructField('channel', StringType(), True),
               StructField('name', StringType(), True),
               StructField('time', StringType(), True),
               StructField('user', StringType(), True)]);
    files = result['files']
    rdd = spark.sparkContext.parallelize(files);
    df = spark.createDataFrame(rdd,schema);
    df = df.withColumn('time',from_unixtime(round(col('timestamp').astype(FloatType())),'yyyy-MM-dd\'T\'HH:mm:ss.000'))
    # df.show()
    collect = df.collect()
    print('files in slack - '+str(len(collect)))
    lc = [];
    for row in collect:
      for channel in row['channels']:
        lc.append({'id': row['id'], 'channel': channel, 'name': row['name'], 'time': row['time'], 'user': row['user']});
    print('files downloaded mapped to channels - '+str(len(lc)))
    rc = spark.sparkContext.parallelize(lc);
    dfc = spark.createDataFrame(rc,sc);
    # print(df.schema);
    # dfc.show();
    pdf = dfc.toPandas();
    dd_writefile(filename,pdf)
    print('end');
  except Exception as err:
    print('Error')
    print(err)

def retrieve_userdata():
  try:
    loop = True;
    files = glob.glob('data/'+batch+'/api/user_list-*.json')
    for file in files:
      print('retrieving user_data - '+file);
      a = 0;
      result = None;
      while a < 3 and result == None:
        try:
          f = open(file,'r');
          result = ast.literal_eval(f.read());
        except Exception as err:
          print('error in file io')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_userdata(result);
  except Exception as err:
    print('Error')
    print(err)

def retrieve_channeldata():
  try:
    loop = True;
    files = glob.glob('data/'+batch+'/api/conversation_list-*.json')
    files.sort(key = sortfile_key)
    # assuming it is in order of "filename" - though there are conflicting messages
    for file in files:
      print('retrieving channel_data - '+file);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          f = open(file,'r');
          result = ast.literal_eval(f.read());
        except Exception as err:
          print('error in file io')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_channeldata(result);
  except Exception as err:
    print('Error')
    print(err)

def retrieve_messages(id):
  try:
    loop = True;
    files = glob.glob('data/'+batch+'/api/conversation_history-'+id+'*.json')
    files.sort(key = sortfile_key)
    for file in files:
      print('retrieving message_data for channel: '+id+' '+file);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          f = open(file,'r');
          result = ast.literal_eval(f.read());
        except Exception as err:
          print('error in file io')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_messagedata(id,result);
  except Exception as err:
    print('Error')
    print(err)

def retrieve_threads(id,ts):
  try:
    loop = True;
    files = glob.glob('data/'+batch+'/api/conversation_replies-'+id+'_'+ts+'*.json')
    files.sort(key = sortfile_key)
    for file in files:
      print('retrieving thread_data for message:'+id+' '+ts+' '+file);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          f = open(file,'r');
          result = ast.literal_eval(f.read());
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_threaddata(id,ts,result);
  except Exception as err:
    print('Error')
    print(err)

def retrieve_filedata():
  try:
    loop = True;
    files = glob.glob('data/'+batch+'/api/files_list-*.json')
    files.sort(key = sortfile_key)
    for file in files:
      print('retrieving file_data: '+file);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          f = open(file,'r');
          result = ast.literal_eval(f.read());
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_filedata(result);
  except Exception as err:
    print('Error')
    print(err)

print('this was executed with batch number '+batch);
try:
  retrieve_userdata();
  retrieve_channeldata();
  retrieve_filedata();
except Exception as err:
  print(err)

