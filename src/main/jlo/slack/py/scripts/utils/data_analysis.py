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

import nltk
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
from nltk.probability import FreqDist
from nltk.stem import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer

ps = PorterStemmer()
lem = WordNetLemmatizer()

import matplotlib.pyplot as plt

# print(certifi.where());
nltk.download('stopwords');
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

def analyse_text(s):
  # print("original text");
  # print(s) 
  word_tokens = word_tokenize(s) 
  # remove punctuations
  word_tokens = [word for word in word_tokens if word.isalnum()]
  # print("tokens");
  # print(word_tokens) 
  stop_words = set(stopwords.words('english')) 
  filtered_sentence = [] 
  filtered_sentence = [w for w in word_tokens if not w in stop_words] 
  # print("tokens without stopwords");
  # print(filtered_sentence) 

  # print("stemming")
  # stemmed_words = []
  # for word in filtered_sentence:
  #  stemmed_words.append(ps.stem(word))
  # print(stemmed_words)

  # print("lemma")
  lemmed_words = []
  for word in filtered_sentence:
    lemmed_words.append(lem.lemmatize(word.lower(),"v"))
  # print(lemmed_words)

  # print("pos")
  # pos_tags = nltk.pos_tag(filtered_sentence)
  # print(pos_tags)
  # if 'thank' in lemmed_words:
  if 'thank' in lemmed_words or 'thx' in lemmed_words:
    return lemmed_words;
  else:
    return None;
