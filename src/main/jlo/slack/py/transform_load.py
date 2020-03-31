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

# ssl_context = ssl.create_default_context(cafile=certifi.where())
spark = SparkSession.builder.master('local').appName('SlackBot').getOrCreate();

batch = None
stage = None
if len(sys.argv) >= 2:
  batch = sys.argv[1]
if len(sys.argv) >= 3:
  stage = int(sys.argv[2])

if batch == None:
  print('no batch id provided');
  exit(-1);
if stage == None:
  print('run all stages');

def sortfile_key(file):
  return int(file[-21:-5]);

def dd_writefile(filename,pdf):
  with open('data/'+batch+'/csv/'+filename+'.csv','a') as f:
    pdf.to_csv(f,index=False,quoting=csv.QUOTE_ALL,mode='a',header=f.tell()==0);

def dd_readfile(filename):
  try:
    pdf = pandas.read_csv('data/'+batch+'/csv/'+filename+'.csv');
    return pdf;
  except Exception as err:
    print(err);

def dd_userdata(result):
  filename = "user_data";
  try:
    users = result['members'];
    pdf = pandas.DataFrame.from_records(users);
    ddpdf = pdf[['id','name','real_name','tz']];
    dd_writefile(filename,ddpdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_channeldata(result):
  filename = "channel_data";
  try:
    channels = result['channels'];
    for channel in channels:
      id = channel['id']
      retrieve_messages(id);
    pdf = pandas.DataFrame.from_records(channels);
    ddpdf = pdf[['id','name']];
    dd_writefile(filename,ddpdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_messagedata(id,result):
  filename = "message_data";
  try:
    messages = result['messages']
    r1 = spark.sparkContext.parallelize(messages);
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
    df1 = spark.createDataFrame(r1,s1);
    for row in df1.collect():
      dd_reactiondata(row)
      if 'reply_count' in df1.columns and row['reply_count'] != None and row['reply_count'] > 0:
        print('replies:'+row['ts']+','+str(row['reply_count']))
        retrieve_threads(id,row['ts']);
      # else:
      #   print('no replies');
    df1 = df1.na.fill({'channel':id})
    df2 = df1.withColumn('time',from_unixtime(round(col('ts').astype(FloatType())),'yyyy-MM-dd\'T\'HH:mm:ss.000'))
    ddpdf = df2.toPandas();
    dd_writefile(filename,ddpdf)
  except Exception as err:
    print('Error - dd_messagedata')
    print(err)

def dd_threaddata(id,ts,result):
  filename = "thread_data";
  try:
    messages = result['messages']
    for row in messages:
      dd_reactiondata(row)
    s1 = StructType([
               StructField('channel', StringType(), True),
               StructField('type', StringType(), True),
               StructField('subtype', StringType(), True),
               StructField('ts', StringType(), True),
               StructField('thread_ts', StringType(), True),
               StructField('time', StringType(), True),
               StructField('user', StringType(), True),
               StructField('text', StringType(), True)]);
    r1 = spark.sparkContext.parallelize(messages);
    df1 = spark.createDataFrame(r1,s1);
    df1 = df1.na.fill({'channel':id})
    df2 = df1.withColumn('time',from_unixtime(round(col('ts').astype(FloatType())),'yyyy-MM-dd\'T\'HH:mm:ss.000'))
    # df2.show()
    pdf = df2.toPandas();
    dd_writefile(filename,pdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_reactiondata(row):
  filename = "reaction_data";
  try:
    # print(row)
    if 'reactions' in row and row['reactions'] != None:
      if 'thread_ts' in row and row['ts'] != row['thread_ts']:
        l = []
        for reaction in row['reactions']:
          for user in reaction['users']:
            l.append({'reaction':str(reaction['name']), 'ts': str(row['ts']), 'user': user});
        pdf = pandas.DataFrame.from_records(l);
        dd_writefile(filename,pdf)
  except Exception as err:
    print('Error - dd_reactiondata')
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
    files = result['files']
    rdd = spark.sparkContext.parallelize(files);
    df = spark.createDataFrame(rdd,schema);
    df = df.withColumn('time',from_unixtime(round(col('timestamp').astype(FloatType())),'yyyy-MM-dd\'T\'HH:mm:ss.000'))
    # df.show()
    lc = [];
    for row in df.collect():
      if 'channels' in row and row['channels'] != None:
        for channel in row['channels']:
          lc.append({'id': row['id'], 'channel': channel, 'name': row['name'], 'time': row['time'], 'user': row['user']});
    print('files downloaded mapped to channels - '+str(len(lc)))
    pdf = pandas.DataFrame.from_records(lc);
    dd_writefile(filename,pdf)
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

def create_conversationdata():
  try:
    channel_data = dd_readfile('channel_data'));
    channel_data = dd_readfile('channel_data'));
    channel_data = dd_readfile('channel_data'));
    channel_data = dd_readfile('channel_data'));
  except Exception as err:
    print('Error');
    print(err);

print('this was executed with batch number '+batch);

if stage == None or stage == 1:
  print('Stage 1 - TRANSFORM / LOAD (INTO CSV)');
  try:
    print('transform and load user data');
    retrieve_userdata();
    print('transform and load channel / message / thread  data');
    retrieve_channeldata();
    print('transform and load file data');
    retrieve_filedata();
  except Exception as err:
    print(err)
    exit(-1);
else:
  print('skipping Stage 1');

if stage == None or stage <= 2:
  print('Stage 2 - CREATE ANALYSIS');
  try:
    # create_conversationdata();
  except Exception as err:
    print(err)
    exit(-1);
else:
  print('skipping Stage 2');
