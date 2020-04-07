import os
import time
import pandas
import numpy
import ast
import ssl
import certifi
import glob
import sys
import json
import itertools
from utils import data_sources as ds
from functools import reduce
from math import floor
from datetime import datetime

sign_token = os.environ['SLACK_SIGN_TOKEN']
access_token = os.environ['SLACK_ACCESS_TOKEN']
user_token = os.environ['SLACK_USER_TOKEN']
# user_token = 'SLACK_USER_TOKEN'  

# ssl_context = ssl.create_default_context(cafile=certifi.where())

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

def dd_userdata(infile,result):
  filename = "user_data";
  try:
    users = result['members'];
    pdf = pandas.DataFrame.from_records(users);
    ddpdf = pdf[['id','name','real_name','tz']];
    ds.dd_writecsv(batch,filename,ddpdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_channeldata(infile,result):
  filename = "channel_data";
  try:
    channels = result['channels'];
    l = []
    for channel in channels:
      id = channel['id'];
      channel_type = ''
      channel_class = ''
      channel_name  = ''
      if 'is_channel' in channel and channel['is_channel']:
        channel_type = 'channel';
      if 'is_mpim' in channel and channel['is_mpim']:
        channel_type = 'mpim';
      if 'is_im' in channel and channel['is_im']:
        channel_type = 'im';
      if 'is_private' in channel and channel['is_private']:
        channel_class = 'confidential';
      else:
        channel_class = 'public';
      if 'name' in channel:
        channel_name = channel['name'] 
      if 'is_archived' in channel:
        is_archived = channel['is_archived'] 
      if 'is_private' in channel:
        is_private = channel['is_private'] 
      l.append({'id': id, 'name': channel_name, 'type': channel_type, 'class': channel_class, 'is_archived': is_archived, 'is_private': is_private });
      # ds.retrieve_messages(id);
      ds.dd_processfile(file_wildcard='data/'+batch+'/api/conversation_history-'+id+'*.json',dd_function=dd_messagedata,id=id);
    ddpdf = pandas.DataFrame.from_records(l);
    if len(ddpdf.index) > 0:
      ds.dd_writecsv(batch,filename,ddpdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_messagedata(id,infile,result):
  filename = "message_data";
  try:
    messages = result['messages']
    for message in result['messages']:
      dd_reactiondata(message)
      if 'reply_count' in message and message['reply_count'] != None and message['reply_count'] > 0:
        print('replies:'+message['ts']+','+str(message['reply_count']));
        # ds.retrieve_threads(id,message['ts']);
        ds.dd_processfile(file_wildcard='data/'+batch+'/api/conversation_replies-'+id+'_'+message['ts']+'*.json',dd_function=dd_threaddata,id=id,ts=message['ts']);
    ddpdf = pandas.DataFrame.from_records(messages);
    if len(ddpdf.index) > 0:
      ddpdf['channel'] = id;
      if 'subtype' not in ddpdf:
        ddpdf['subtype'] = None;
      if 'reply_count' not in ddpdf:
        ddpdf['reply_count'] = None;
      if 'thread_ts' not in ddpdf:
        ddpdf['thread_ts'] = None;
      if 'user' not in ddpdf:
        ddpdf['user'] = None;
      if 'text' not in ddpdf:
        ddpdf['text'] = None;
      ddpdf['time'] = ddpdf['ts'].apply(ds.epochstr_to_isostr);
      ddpdf = ddpdf[['channel','type','subtype','ts','thread_ts','time','reply_count','user','text']];
      ds.dd_writecsv(batch,filename,ddpdf)
  except Exception as err:
    print('Error - dd_messagedata')
    print(err)

def dd_threaddata(id,ts,infile,result):
  filename = "thread_data";
  try:
    messages = result['messages']
    for row in messages:
      dd_reactiondata(row)
    ddpdf = pandas.DataFrame.from_records(messages);
    if len(ddpdf.index) > 0:
      ddpdf['channel'] = id;
      if 'subtype' not in ddpdf:
        ddpdf['subtype'] = None;
      if 'user' not in ddpdf:
        ddpdf['user'] = None;
      if 'text' not in ddpdf:
        ddpdf['text'] = None;
      ddpdf['time'] = ddpdf['ts'].apply(ds.epochstr_to_isostr);
      ddpdf = ddpdf[['channel','type','subtype','ts','thread_ts','time','user','text']];
      # print(ddpdf);
      ds.dd_writecsv(batch,filename,ddpdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_reactiondata(row):
  filename = "reaction_data";
  try:
    # print(row)
    if 'reactions' in row and row['reactions'] != None:
      if not('thread_ts' in row) or row['ts'] != row['thread_ts']:
        l = []
        for reaction in row['reactions']:
          for user in reaction['users']:
            l.append({'reaction':str(reaction['name']), 'ts': str(row['ts']), 'user': user});
        pdf = pandas.DataFrame.from_records(l);
        if len(pdf.index) > 0:
          ds.dd_writecsv(batch,filename,pdf)
  except Exception as err:
    print('Error - dd_reactiondata')
    print(err)

def dd_filedata(infile,result):
  filename = "file_data";
  try:
    files = result['files']
    lc = [];
    for row in files:
      if 'channels' in row and row['channels'] != None:
        for channel in row['channels']:
          lc.append({'id': row['id'], 'channel': channel, 'name': row['name'], 'time': ds.epochstr_to_isostr(row['timestamp']), 'user': row['user']});
    print('files downloaded mapped to channels - '+str(len(lc)))
    pdf = pandas.DataFrame.from_records(lc);
    ds.dd_writecsv(batch,filename,pdf)
  except Exception as err:
    print('Error')
    print(err)

def dd_polldata(id,infile,result):
  filename = "poll_data";
  try:
    messages = result['messages']
    lc = [];
    poll_id = None;
    poll_text = None;
    for message in messages:
      if 'subtype' in message and message['subtype'] == 'bot_message' and 'blocks' in message and message['blocks'] != None:
        for block in message['blocks']:
          if 'block_id' in block and block['block_id'][0:5] == 'poll-':
            print(block['block_id']);
            if block['block_id'][-4:] == 'menu':
              poll_id = block['block_id'][:-15];
              poll_text = block['text']['text'];
            else:
              print(poll_id);
              print(block['text']['text'].split('`'));
              vote = block['text']['text'].split('`');
              lc.append({'poll_id': poll_id, 'ts': message['ts'], 'time': ds.epochstr_to_isostr(message['ts']), 'text': poll_text, 'vote_item': vote[0].strip(), 'vote_count': int(vote[1]) if len(vote) > 1 else 0 });
    pdf = pandas.DataFrame.from_records(lc);
    ds.dd_writecsv(batch,filename,pdf)
  except Exception as err:
    print('Error - dd_polldata')
    print(err)

def dd_jsondata(infile,result):
  try:
    json_result = json.dumps(result)
    outfile = infile.replace('/api/','/json/');
    ds.dd_writefile(outfile,json_result);
  except Exception as err:
    print('Error - dd_jsondata')
    print(err)

def convert_to_json():
  try:
    loop = True;
    try:
      os.makedirs('data/'+batch+'/json');
    except Exception as err:
      print('directory exists');
    files = glob.glob('data/'+batch+'/api/*.json')
    for file in files:
      print('retrieving file_data: '+file);
      a = 0;
      result = None
      while a < 3 and result == None:
        try:
          fi = open(file,'r');
          result = ast.literal_eval(fi.read());
          json_result = json.dumps(result)
          outfile = file.replace('/api/','/json/');
          fo = open(outfile,'w')
          fo.write(str(json_result))
          fo.close();
          fi.close();
        except Exception as err:
          print('error in file conversion')
          print(err);
          a += 1
          result = None
          time.sleep(0.1)
      if result == None:
        exit(-1);
  except Exception as err:
    print('Error')
    print(err)

def create_conversationdata():
  filename = "conversation_data";
  try:
    message_pdf = ds.dd_readcsv(batch,'message_data');
    user_pdf = ds.dd_readcsv(batch,'user_data');
    thread_pdf = ds.dd_readcsv(batch,'thread_data');

    zone_ref_pdf = ds.dd_readref('zone');
    country_ref_pdf = ds.dd_readref('country');

    # Use only subset of information (and create identical columns)
    message_part_pdf = message_pdf[['channel','type','subtype','ts','thread_ts','time','user','text']];
    thread_part_pdf = thread_pdf[['channel','type','subtype','ts','thread_ts','time','user','text']];
    # Merge message and thread data
    conversation_part_pdf = message_part_pdf.append(thread_part_pdf,ignore_index=False);
    conversation_part_pdf = conversation_part_pdf.drop_duplicates();
    # From user data, split as a method to create default country (if timezone is not recognised).
    tz_split = user_pdf['tz'].str.split('/',expand=True);
    # Merge to create richer iso information
    iso_ref_pdf = zone_ref_pdf.merge(country_ref_pdf,left_on='ISO',right_on='ISO2');
    # Merge iso information with user (based upon timezone).
    user_geo_pdf = user_pdf.merge(iso_ref_pdf,how='left',left_on='tz',right_on='TZ');
    # Add country and city based upon user data timezone
    user_geo_pdf['country']  = tz_split[0];
    user_geo_pdf['city']  = tz_split[1];
    # Mask NAN values of country based upon timezone information
    user_geo_pdf['COUNTRY'] = user_geo_pdf['COUNTRY'].mask(pandas.isnull, user_geo_pdf['country']);
    # Merge user data with conversation data based upon user id
    conversation_geo_pdf = conversation_part_pdf.merge(user_geo_pdf,how='left',left_on='user',right_on='id');
    # Add a column (replica of ts) that can be used as an number
    conversation_geo_pdf['ts_int'] = conversation_geo_pdf['ts']
    # Select columns
    conversation_pdf = conversation_geo_pdf[['channel','type','subtype','ts','thread_ts','ts_int','time','user','real_name','name','text','city','COUNTRY','ISO']];
    # Rename columns
    ddpdf = conversation_pdf.rename(columns={'channel':'CHANNEL','type':'TYPE','subtype':'SUBTYPE','ts':'TS','thread_ts':'THREAD_TS','ts_int':'TS_INT','time':'TIME','user':'USER','real_name':'REAL_NAME','name':'NAME','text':'TEXT','city':'CITY','COUNTRY':'COUNTRY','ISO':'ISO'});

    ds.dd_writecsv(batch,filename,ddpdf);
  except Exception as err:
    print('Error');
    print(err);

def create_nodedata():
  filename = "node_data";
  try:
    channel_pdf = ds.dd_readcsv(batch,'channel_data');
    user_pdf = ds.dd_readcsv(batch,'user_data');

    zone_ref_pdf = ds.dd_readref('zone');
    country_ref_pdf = ds.dd_readref('country');

    # From user data, split as a method to create default country (if timezone is not recognised).
    tz_split = user_pdf['tz'].str.split('/',expand=True);
    # Merge to create richer iso information
    iso_ref_pdf = zone_ref_pdf.merge(country_ref_pdf,left_on='ISO',right_on='ISO2');
    # Merge iso information with user (based upon timezone).
    user_geo_pdf = user_pdf.merge(iso_ref_pdf,how='left',left_on='tz',right_on='TZ');
    # Augment columns for union 
    # Add country and city based upon user data timezone
    user_geo_pdf['country']  = tz_split[0];
    user_geo_pdf['city']  = tz_split[1];
    # Mask NAN values of country based upon timezone information
    user_geo_pdf['COUNTRY'] = user_geo_pdf['COUNTRY'].mask(pandas.isnull, user_geo_pdf['country']);
    # Add "null" columns for union
    user_geo_pdf['type'] = 'user'
    user_geo_pdf['channel_type'] = None
    user_geo_pdf['channel_class'] = None
    user_part_pdf = user_geo_pdf[['id','name','type','real_name','ISO','COUNTRY','city','channel_type','channel_class']];
    # Augment columns for union 
    channel_pdf = channel_pdf.rename(columns={'type':'channel_type','class':'channel_class'});
    channel_pdf['type'] = 'channel'
    channel_pdf['name'] = channel_pdf['name'].mask(pandas.isnull, channel_pdf['id']);
    channel_pdf['real_name'] = channel_pdf['name']
    # Add "null" columns for union
    channel_pdf['timezone'] = None
    channel_pdf['ISO'] = None
    channel_pdf['COUNTRY'] = None
    channel_pdf['city'] = None
    channel_part_pdf = channel_pdf[['id','name','type','real_name','ISO','COUNTRY','city','channel_type','channel_class']];
    # Merge user and channel nodes
    ddpdf = user_part_pdf.append(channel_part_pdf,ignore_index=True);
    # Rename columns
    ddpdf = ddpdf.rename(columns={'id':'ID','name':'LABEL','type':'TYPE','real_name':'NAME','ISO':'ISO','COUNTRY':'COUNTRY','city':'CITY','channel_type':'CHANNEL_TYPE','channel_class':'CHANNEL_CLASS'});

    ds.dd_writecsv(batch,filename,ddpdf);
  except Exception as err:
    print('Error');
    print(err);

def create_edgedata():
  filename = "edge_data";
  try:
    conversation_pdf = ds.dd_readcsv(batch,'conversation_data');
    c1_pdf = conversation_pdf.copy();
    reaction_pdf = ds.dd_readcsv(batch,'reaction_data');
    # c1_pdf = c1_pdf[c1_pdf['SUBTYPE'] == 'thread_broadcast' or c1_pdf['SUBTYPE'] == None];
    c1_pdf_1 = c1_pdf[c1_pdf['SUBTYPE'] == 'thread_broadcast'];
    c1_pdf_2 = c1_pdf[c1_pdf['SUBTYPE'].isnull()];
    c1_pdf = c1_pdf_1.append(c1_pdf_2,ignore_index=False);
    c2_pdf = c1_pdf.copy();
    c2_pdf = c2_pdf[c2_pdf['THREAD_TS'].notnull()];
    c4_pdf = c1_pdf.merge(c2_pdf,how='left',left_on='THREAD_TS',right_on='TS');
    # print(c4_pdf.shape);
    c4_pdf = c4_pdf[c4_pdf['USER_x'] != c4_pdf['USER_y']];
    c4_pdf = c4_pdf[['CHANNEL_x','USER_x','USER_y']];
    c4_pdf['USER_y'] = c4_pdf['USER_y'].mask(pandas.isnull, c4_pdf['CHANNEL_x']);
    c4_pdf['RELATE'] = 'interacts';
    c4_pdf = c4_pdf.rename(columns={'CHANNEL_x':'CHANNEL','USER_x':'SOURCE','USER_y':'TARGET','RELATE':'RELATE'});
    # print(c4_pdf.shape);
    c3_pdf = c1_pdf.copy();
    c5_pdf = c3_pdf.merge(reaction_pdf,how='left',left_on='TS',right_on='ts');
    c5_pdf = c5_pdf[['CHANNEL','USER','user']];
    c5_pdf['RELATE'] = 'reacts';
    c5_pdf = c5_pdf.rename(columns={'CHANNEL':'CHANNEL','USER':'SOURCE','user':'TARGET','RELATE':'RELATE'});
    # print(c5_pdf.shape);
    ddpdf = c4_pdf.append(c5_pdf,ignore_index=True);
    # print(ddpdf);

    ds.dd_writecsv(batch,filename,ddpdf);
  except Exception as err:
    print('Error');
    print(err);

def aggregate_conversationdata():
  try:
    channel_pdf = ds.dd_readcsv(batch,'channel_data');
    user_pdf = ds.dd_readcsv(batch,'user_data');

    c1_pdf = ds.dd_readcsv(batch,'conversation_data');
    c1_pdf_1 = c1_pdf[c1_pdf['SUBTYPE'] == 'thread_broadcast'];
    c1_pdf_2 = c1_pdf[c1_pdf['SUBTYPE'].isnull()];
    c1_pdf = c1_pdf_1.append(c1_pdf_2,ignore_index=False);
    agg_pdf = c1_pdf.groupby('CHANNEL').count()[['TS','THREAD_TS']];
    # print(agg_pdf);
    ds.dd_writecsv(batch,'aggr_by_channel',agg_pdf,True);
    agg_pdf = c1_pdf.groupby(['CHANNEL','USER']).count()[['TS','THREAD_TS']];
    agg_pdf['POSTS'] = agg_pdf['TS'] + agg_pdf['THREAD_TS'];
    # print(agg_pdf);
    ds.dd_writecsv(batch,'aggr_by_channel_user',agg_pdf,True);
    unique_pdf = agg_pdf.copy();
    unique_pdf.reset_index(inplace=True);
    unique_pdf = unique_pdf.groupby(['CHANNEL']).count()[['USER']];
    print(unique_pdf);
    ds.dd_writecsv(batch,'number_channel_user',unique_pdf,True);
    max_pdf = agg_pdf.groupby(['CHANNEL']).max()[['POSTS']];
    # print(max_pdf);
    m1_pdf = agg_pdf.merge(max_pdf,how='inner',left_index=True,right_index=True);
    m1_pdf = m1_pdf[m1_pdf['POSTS_x'] == m1_pdf['POSTS_y']];
    print(m1_pdf);
    m1_pdf.reset_index(inplace=True);
    m2_pdf = m1_pdf.merge(user_pdf,how='left',right_on='id',left_on='USER');
    m3_pdf = m2_pdf.merge(channel_pdf,how='left',right_on='id',left_on='CHANNEL');
    m4_pdf = m3_pdf[['CHANNEL','name_y','USER','real_name','POSTS_x']];
    m4_pdf = m4_pdf.rename(columns={'CHANNEL':'CHANNEL_ID','name_y':'CHANNEL_NAME','USER':'USER_ID','real_name':'USER_NAME','POSTS_x':'POSTS'});
    # print(m4_pdf);
    ds.dd_writecsv(batch,'max_by_channel_user',m4_pdf);
  except Exception as err:
    print('Error');
    print(err);

def create_timediff_conversations():
  filename = "conversation_timediff_data";
  try:
    df = ds.dd_readcsv(batch,'conversation_data');
    df = df.sort_values(['CHANNEL','TS'],ascending=(True,True));
    df['TIME_TS'] = pandas.to_datetime(df['TIME']);
    # print(df);
    ddf = df[['CHANNEL','TIME_TS']];
    # print(ddf);
    # print(ddf.dtypes);
    ddf = ddf.groupby('CHANNEL').diff();
    ddf['TIME_TS'] = ddf['TIME_TS']  / numpy.timedelta64(1, 's');
    ddf['TIME_TS'] = ddf['TIME_TS'].mask(pandas.isnull, 0);
    # print(ddf);
    # print(ddf.dtypes);
    df = df.merge(ddf,left_index=True,right_index=True);
    # print(df);
    # print(df.dtypes);
    ddpdf = df[['CHANNEL','TYPE','SUBTYPE','TS','THREAD_TS','TS_INT','TIME','USER','REAL_NAME','NAME','TEXT','CITY','COUNTRY','ISO','TIME_TS_y']];
    ddpdf = ddpdf.rename(columns={'TIME_TS_y':'DIFF'});
    # print(ddpdf);
    # print(ddpdf.dtypes);
    ds.dd_writecsv(batch,filename,ddpdf);
  except Exception as err:
    print('Error');
    print(err);

def create_timediff_threads():
  filename = "threads_timediff_data";
  try:
    df = ds.dd_readcsv(batch,'conversation_data');
    df = df[df['THREAD_TS'].notnull()];
    df = df.sort_values(['CHANNEL','THREAD_TS','TS'],ascending=(True,True,True));
    df['TIME_TS'] = pandas.to_datetime(df['TIME']);
    # print(df);
    ddf = df[['CHANNEL','THREAD_TS','TIME_TS']];
    # print(ddf);
    # print(ddf.dtypes);
    ddf = ddf.groupby(['CHANNEL','THREAD_TS']).diff();
    ddf['TIME_TS'] = ddf['TIME_TS']  / numpy.timedelta64(1, 's');
    ddf['TIME_TS'] = ddf['TIME_TS'].mask(pandas.isnull, 0);
    # print(ddf);
    # print(ddf.dtypes);
    df = df.merge(ddf,left_index=True,right_index=True);
    # print(df);
    # print(df.dtypes);
    ddpdf = df[['CHANNEL','TYPE','SUBTYPE','TS','THREAD_TS','TS_INT','TIME','USER','REAL_NAME','NAME','TEXT','CITY','COUNTRY','ISO','TIME_TS_y']];
    ddpdf = ddpdf.rename(columns={'TIME_TS_y':'DIFF'});
    # print(ddpdf);
    # print(ddpdf.dtypes);
    ds.dd_writecsv(batch,filename,ddpdf);
  except Exception as err:
    print('Error');
    print(err);

def aggregate_threads():
  filename = "length_threads_data";
  try:
    df = ds.dd_readcsv(batch,'conversation_data');
    df = df[df['THREAD_TS'].notnull()];
    ddf = df.groupby(['CHANNEL','THREAD_TS']).count()[['TS']];
    ddf.reset_index(inplace=True);
    print(ddf);
    ds.dd_writecsv(batch,'aggr_by_thread',ddf);
    ddf = df.groupby(['CHANNEL','THREAD_TS','USER']).count()[['TS']];
    ddf.reset_index(inplace=True);
    print(ddf);
    ds.dd_writecsv(batch,'aggr_by_thread_user',ddf);
  except Exception as err:
    print('Error');
    print(err);

print('this was executed with batch number '+batch);

def exec_stages():
  if stage == None or stage == 1:
    print('Stage 1 - TRANSFORM / LOAD (INTO CSV AND JSON)');
    try:
      print('transform and load user data');
      # ds.retrieve_userdata();
      ds.dd_processfile(file_wildcard='data/'+batch+'/api/user_list*.json',dd_function=dd_userdata);
      print('transform and load channel / message / thread  data');
      # ds.retrieve_channeldata();
      ds.dd_processfile(file_wildcard='data/'+batch+'/api/conversation_list*.json',dd_function=dd_channeldata);
      print('transform and load file data');
      # ds.retrieve_filedata();
      ds.dd_processfile(file_wildcard='data/'+batch+'/api/files_list-*.json',dd_function=dd_filedata);
      print('transform and load poll data from a specific bot id: (argh - fragile) B0115K4AY5B');
      # ds.retrieve_polldata('B0115K4AY5B');
      ds.dd_processfile(file_wildcard='data/'+batch+'/api/conversation_history-C010VKAQ96V*.json',dd_function=dd_polldata,id='B0115K4AY5B');
      # convert_to_json();
      ds.dd_processfile(file_wildcard='data/'+batch+'/api/*.json',dd_function=dd_jsondata);
    except Exception as err:
      print(err)
      exit(-1);
  else:
    print('skipping Stage 1');

  if stage == None or stage <= 2:
    print('Stage 2 - CREATE ANALYSIS');
    try:
      create_conversationdata();
      create_timediff_conversations();
      create_nodedata();
      create_edgedata();
      print("");
    except Exception as err:
      print(err)
      exit(-1);
  else:
    print('skipping Stage 2');

  if stage == None or stage <= 3:
    print('Stage 3 - CREATE AGGREGATES');
    try:
      aggregate_conversationdata();
      aggregate_threads();
      print("");
    except Exception as err:
      print(err)
      exit(-1);
  else:
    print('skipping Stage 3');
  
exec_stages();
