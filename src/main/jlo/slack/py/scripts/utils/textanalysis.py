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
# nltk.download('stopwords');
# nltk.download('punkt')
# nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

example_sent = "What makes a great entreprenur? Here's a start but authors note the value of grit and tends to understate the role of culture. For example, I'm working with various social impact and they think B-corp or constitutional amendments without understanding external certification and stated goals are incomplete without constant reinforcement of the values and rituals of the group. I believe the conventional wisdom is that for governance ...\n"+ \
"- < 10 people ... assignment by tasks\n"+ \
"- 10-100 employees ... by responsibility (sales, R&D etc)\n"+ \
"- 100-1000 ... pseudometrics - scorecards, KPIs etc\n"+ \
"- 1000+ culture\n"+ \
"\n"+ \
"Which may be why 100k employees firms have a hard time innovating (in creating new market categories, not just invention).";

print("original text");
print(example_sent) 

word_tokens = word_tokenize(example_sent) 
# remove punctuations
word_tokens = [word for word in word_tokens if word.isalnum()]
print("tokens");
print(word_tokens) 

stop_words = set(stopwords.words('english')) 
filtered_sentence = [] 
filtered_sentence = [w for w in word_tokens if not w in stop_words] 
print("tokens without stopwords");
print(filtered_sentence) 

fdist = FreqDist(filtered_sentence)
print(fdist)
fdist.plot(30,cumulative=False)
plt.show()

print("stemming")
stemmed_words = []
for word in filtered_sentence:
  stemmed_words.append(ps.stem(word))
print(stemmed_words)

print("lemma")
lemmed_words = []
for word in filtered_sentence:
  lemmed_words.append(lem.lemmatize(word,"v"))
print(lemmed_words)

print("pos")
pos_tags = nltk.pos_tag(filtered_sentence)
print(pos_tags)
