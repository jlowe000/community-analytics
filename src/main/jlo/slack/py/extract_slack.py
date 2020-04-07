import os
import time
import csv
import slack
import pandas
import urllib3
import ssl
import certifi
from math import floor
from datetime import datetime
# from pyspark.sql.functions import expr
# from pyspark.sql.functions import round
# from pyspark.sql.functions import lit
# from pyspark.sql.functions import col
# from pyspark.sql.functions import from_unixtime
# from pyspark.sql.types import FloatType
# from pyspark.sql.types import IntegerType
# from pyspark.sql.types import StringType
# from pyspark.sql.types import ArrayType
# from pyspark.sql.types import StructType
# from pyspark.sql.types import StructField
# from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf

sign_token = os.environ['SLACK_SIGN_TOKEN']
access_token = os.environ['SLACK_ACCESS_TOKEN']
user_token = os.environ['SLACK_USER_TOKEN']
# user_token = 'SLACK_USER_TOKEN'  

ssl_context = ssl.create_default_context(cafile=certifi.where())
client = slack.WebClient(token=access_token,ssl=ssl_context)
# spark = SparkSession.builder.master('local').appName('SlackBot').getOrCreate();
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where());

batchtime = datetime.now();
# batch = floor(batchtime.timestamp()*1000)
batch = batchtime.strftime('%Y%m%d%H%M%S')

def dd_writemetadata():
  os.makedirs('data/'+batch+'/api');
  os.makedirs('data/'+batch+'/csv');
  os.makedirs('data/'+batch+'/json');
  os.makedirs('data/'+batch+'/metadata');
  f = open('data/'+batch+'/metadata/metadata.csv','a');
  f.write('"time"\n"'+batchtime.isoformat(timespec='seconds')+'"\n');
  f.close();

def dd_writejson(filename,key,result):
  current_time = floor(datetime.now().timestamp()*1000000)
  f = open('data/'+batch+'/api/'+filename+'-'+key+'-'+str(current_time)+'.json','a');
  f.write(str(result));
  f.close();

def parse_channeldata(result):
  filename = "channel_data";
  try:
    channels = result['channels'];
    for channel in channels:
      filter = False;
      if 'is_mpim' in channel and channel['is_mpim']:
        filter = True;
      if 'is_im' in channel and channel['is_im']:
        filter = True;
      if 'is_private' in channel and channel['is_private']:
        filter = True;
      retrieve_messages(channel['id'],filter);
  except Exception as err:
    print('Error')
    print(err)

def parse_messagedata(id,result,to_filter):
  filename = "message_data";
  try:
    messages = result['messages']
    for row in messages:
      print(row['ts']);
      try:
        print('replies:'+row['ts']+','+str(row['reply_count']))
        if row['reply_count'] !=  None and row['reply_count'] > 0:
          retrieve_threads(id,row['ts'],to_filter);
      except Exception as err:
        print('no replies')
  except Exception as err:
    print('Error')
    print(err)

def parse_filedata(result):
  filename = "file_data";
  try:
    files = result['files']
    for file in files:
      if len(file['channels']) > 0:
        res = http.request('GET',file['url_private_download'],headers = { 'Authorization': 'Bearer '+user_token });
        os.makedirs('data/'+batch+'/files/'+file['id']);
        f = open('data/'+batch+'/files/'+file['id']+'/'+file['name'],'ab');
        f.write(res.data);
        f.close();
        print(file['url_private_download'])
      else:
        print('ignored file as deemed as private in message');
    print('end');
  except Exception as err:
    print('Error')
    print(err)

def filter_text(result):
  for message in result['messages']:
    if 'text' in message:
      message['text'] = '';
    if 'blocks' in message:
      message['blocks'] = [];
    if 'files' in message:
      message['files'] = [];
    if 'attachments' in message:
      message['attachments'] = [];
  return result;

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
      dd_writejson('user_list','all',result);
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
          result = client.conversations_list(token=user_token, types='public_channel,private_channel,mpim,im', cursor=cursor);
        except Exception as err:
          print('error in slack call')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
      dd_writejson('conversation_list','all',result);
      parse_channeldata(result);
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

def retrieve_threads(id,ts,to_filter):
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
      if to_filter:
        print ('filtering messages:'+id);
        result = filter_text(result);
      dd_writejson('conversation_replies',id+'_'+ts,result);
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

def retrieve_messages(id,to_filter):
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
      if to_filter:
        print ('filtering messages:'+id);
        result = filter_text(result);
      dd_writejson('conversation_history',id,result);
      parse_messagedata(id,result,to_filter);
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
      dd_writejson('files_list','all',result);
      # print(result)
      parse_filedata(result);
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
print('this was executed with batch number '+batch);
