import os
import time
import csv
import slack
import pandas
import urllib3
import certifi
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

client = slack.WebClient(token=access_token)
spark = SparkSession.builder.master('local').appName('SlackBot').getOrCreate();
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where());

batchtime = datetime.now();
batch = floor(batchtime.timestamp()*1000)

def dd_writemetadata():
  os.makedirs('data/'+str(batch)+'/csv');
  os.makedirs('data/'+str(batch)+'/api');
  f = open('data/'+str(batch)+'/csv/metadata.csv','a');
  f.write('"time"\n"'+batchtime.isoformat(timespec='seconds')+'"\n');
  f.close();

def dd_writejson(filename,result):
  current_time = floor(datetime.now().timestamp()*1000000)
  f = open('data/'+str(batch)+'/api/'+filename+'_'+str(current_time)+'.json','a');
  f.write(str(result));
  f.close();

def dd_writefile(filename,pdf):
  with open('data/'+str(batch)+'/csv/'+filename+'.csv','a') as f:
    pdf.to_csv(f,index=False,quoting=csv.QUOTE_ALL,mode='a',header=f.tell()==0);

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
    # print(df.schema);
    # df.show();
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
    # print(df.schema);
    # df.show();
    pdf = df.toPandas();
    dd_writefile(filename,pdf)
    for id in pdf['id'].values:
      retrieveMessages(id);
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
    reactions = row['reactions'];
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
        retrieveThreads(id,row['ts']);
    pdf.drop(columns=['reactions']);
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
    dd_writefile(filename,pdf)
    print('end');
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
    lc = [];
    for row in collect:
      for channel in row['channels']:
        lc.append({'id': row['id'], 'channel': channel, 'name': row['name'], 'time': row['time'], 'user': row['user']});
    rc = spark.sparkContext.parallelize(lc);
    dfc = spark.createDataFrame(rc,sc);
    # print(df.schema);
    # dfc.show();
    pdf = dfc.toPandas();
    dd_writefile(filename,pdf)
    for file in files:
      res = http.request('GET',file['url_private_download'],headers = { 'Authorization': 'Bearer '+user_token });
      os.makedirs('data/'+str(batch)+'/files/'+file['id']);
      f = open('data/'+str(batch)+'/files/'+file['id']+'/'+file['name'],'ab');
      f.write(res.data);
      f.close();
      print(file['url_private_download'])
    print('end');
  except Exception as err:
    print('Error')
    print(err)

def retrieve_userdata():
  try:
    loop = True;
    cursor = '';
    while loop: 
      print('retrieving user_data');
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          result = client.users_list(token=user_token, cursor=cursor);
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_writejson('user_list',result);
      dd_userdata(result);
      try:
        cursor = result['response_metadata']['next_cursor'];
      except Exception as err:
        cursor = ''
        print('metadata not found');
      loop = result['has_more'];
      print('cursor:'+cursor);
      print('loop:'+str(loop));
  except Exception as err:
    print('Error')
    print(err)

def retrieve_channeldata():
  try:
    loop = True;
    cursor = '';
    while loop: 
      print('retrieving channel_data');
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          result = client.conversations_list(token=user_token, cursor=cursor);
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_writejson('conversation_list',result);
      dd_channeldata(result);
      try:
        cursor = result['response_metadata']['next_cursor'];
      except Exception as err:
        cursor = ''
        print('metadata not found');
      loop = result['has_more'];
      print('cursor:'+cursor);
      print('loop:'+str(loop));
  except Exception as err:
    print('Error')
    print(err)

def retrieveThreads(id,ts):
  try:
    loop = True;
    cursor = '';
    while loop: 
      print('retrieving thread_data for message:'+id+','+ts);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          result = client.conversations_replies(token=user_token, channel=id, ts=ts, cursor=cursor);
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_writejson('conversation_replies',result);
      dd_threaddata(id,ts,result);
      try:
        cursor = result['response_metadata']['next_cursor'];
      except Exception as err:
        cursor = ''
        print('metadata not found');
      loop = result['has_more'];
      print('cursor:'+cursor);
      print('loop:'+str(loop));
  except Exception as err:
    print('Error')
    print(err)

def retrieveMessages(id):
  try:
    loop = True;
    cursor = '';
    while loop: 
      print('retrieving message_data for channel:'+id);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          result = client.conversations_history(token=user_token, channel=id, cursor=cursor);
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_writejson('conversation_history',result);
      dd_messagedata(id,result);
      try:
        cursor = result['response_metadata']['next_cursor'];
      except Exception as err:
        cursor = ''
        print('metadata not found');
      loop = result['has_more'];
      print('cursor:'+cursor);
      print('loop:'+str(loop));
  except Exception as err:
    print('Error')
    print(err)

def retrieve_filedata():
  try:
    loop = True;
    cursor = 1;
    while loop: 
      print('retrieving file_data');
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          result = client.files_list(token=user_token,page=cursor);
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_writejson('files_list',result);
      # print(result)
      dd_filedata(result);
      try:
        pages = result['paging']['pages'];
        page = result['paging']['page'];
        if page < pages:
          cursor = page + 1
        loop = page < pages
      except Exception as err:
        cursor = 1
        loop = False
        print('metadata not found');
      print('cursor:'+str(cursor));
      print('loop:'+str(loop));
  except Exception as err:
    print('Error')
    print(err)

dd_writemetadata();
retrieve_userdata();
retrieve_channeldata();
retrieve_filedata();
