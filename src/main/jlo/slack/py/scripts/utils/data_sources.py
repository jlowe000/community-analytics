import os
import csv
import ast
import time
import pandas
import numpy
import sys
import json
import glob

def sortfile_key(file):
  return int(file[-21:-5]);

def dd_writecsv(batch,filename,pdf,index=False):
  with open('data/'+batch+'/csv/'+filename+'.csv','a') as f:
    pdf.to_csv(f,index=index,quoting=csv.QUOTE_ALL,mode='a',header=f.tell()==0);

def dd_writefile(filename,result):
  with open(filename,'a') as f:
    f.write(result)

def dd_readcsv(batch,filename):
  try:
    pdf = pandas.read_csv('data/'+batch+'/csv/'+filename+'.csv');
    return pdf;
  except Exception as err:
    print(err);

def dd_readref(filename):
  try:
    pdf = pandas.read_csv('../../../../../data/'+filename+'.csv');
    return pdf;
  except Exception as err:
    print(err);

def epochstr_to_isostr(s):
  return time.strftime('%Y-%m-%dT%H:%M:%S.000',time.localtime(float(s)));

def dd_processfile(file_wildcard,dd_function,**kwargs):
  try:
    loop = True;
    files = glob.glob(file_wildcard)
    files.sort(key = sortfile_key)
    for file in files:
      print('retrieving file for : '+file_wildcard+' '+file);
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
      kwargs.update({'infile': file});
      kwargs.update({'result': result});
      return dd_function(**kwargs);
  except Exception as err:
    print('Error')
    print(err)

def dd_test(arg1, arg2, result):
  print(arg1+','+arg2);
  print(result);

def main():
  retrieve_data(file_wildcard='data/test-003/api/user*.json',dd_function=dd_test,arg1='1',arg2='2');

if __name__ == "__main__":
  main()
