from doctest import debug_script
import numpy as np
import pandas as pd
import re
import texthero as hero
from thefuzz import fuzz, process
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk import bigrams
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize 
from nltk.util import ngrams
import json

stop_en = set(stopwords.words('english')) 
stop_fr = set(stopwords.words('french') + ['journal','review'])

def remove_stopwords_fr(s):
    retained_words = [word for word in s.split() if word not in (stop_fr)]
    return ' '.join(retained_words)

def remove_special_characters(s):
    pattern = r'[^a-zA-Z]'
    text = re.sub(pattern, ' ', s)
    return text

def lemmatize(r):
    lemmatizer = WordNetLemmatizer()
    words = nltk.word_tokenize(r)
    lemmatized_words = [lemmatizer.lemmatize(word) for word in words]
    return " ".join(lemmatized_words)

def str2list(s):
    l = s.split(",")
    return [pd.Series([sub]).apply(remove_stopwords_fr).pipe(hero.clean) for sub in l]

def list2str(l):
    return [','.join(sub) for sub in l]

def process_fuzzy_score_uca_developpee(l):
    #l = [s.replace("'","") for s in l]
    rules = [process.extractOne("universite cote azur",l),
               process.extractOne("*universit* *cote *azur",l),
               process.extractOne("universite cote 'azur",l),
               process.extractOne("universite' cote 'azur",l),
               process.extractOne("universit cote 'azur",l),
               process.extractOne("universit c azur",l),
               process.extractOne("universit c 'azur",l),
               process.extractOne("universite cote dazur",l),
               process.extractOne("universite co te dazur",l),
               process.extractOne("universite cte azur",l),
               process.extractOne("universite cote azure",l),
               process.extractOne("university cote azur",l), 
			   process.extractOne("university cote 'azur",l),
               process.extractOne("university cote dazur",l),  
               process.extractOne("universite azur",l),
               process.extractOne("cote azur university",l),
               process.extractOne("cote 'azur university",l), 			   
               process.extractOne("univ cote azur",l),
               process.extractOne("universitte azur",l),
               process.extractOne("universiteco te azur",l),
               process.extractOne("universitcrossed sign cote azur",l),
               process.extractOne("universitcrossed cote azur",l),
               process.extractOne("universit x00e9 c x00f4 te x2019 azur",l),
               process.extractOne("c x00f4 te x0027 azur university",l),
               process.extractOne("universite acute co circ te azur",l),
               process.extractOne("universita(c) ca'te daEUR(tm)azur",l),
               process.extractOne("universiti? 1/2 ci? 1/2 te azur",l),
               process.extractOne("universitc(c) cc'te azur",l),
               process.extractOne("universitecote azur",l)]
    scores_list = []
    for r in rules:
        scores_list.append(r)
    max_tuple = max(scores_list, key=lambda tup: tup[1])
    return max_tuple

def process_uca_position(t,l):
    if t[0] == l[0]:
        return "debut"
    else:
        return "interne"

def process_fuzzy_score_uns(l):
    return max(process.extractOne("universite nice",l)[1],
               process.extractOne("universite nice sophia",l)[1],
               process.extractOne("nice sophia antipolis",l)[1],
               process.extractOne("universite sophia antipolis",l)[1],
               process.extractOne("universite nice sophia antipolis",l)[1],
               process.extractOne("university nice",l)[1],
               process.extractOne("u nice",l)[1],
               process.extractOne("university nice sophia",l)[1],
               process.extractOne("university sophia antipolis",l)[1],
               process.extractOne("university hospital nice",l)[1],
               process.extractOne("sophia antipolis university",l)[1],
               process.extractOne("antipolis university",l)[1],
               process.extractOne("nice university",l)[1],
               process.extractOne("univ nice",l)[1],
               process.extractOne("nice universite campus",l)[1],
               fuzz.partial_ratio("uns",l),
               process.extractOne("unsa",l)[1],
               fuzz.partial_ratio("unsa",l))

def process_fuzzy_sigle(x,sigle):
  #line_ngrams = get_ngrams(x,1)
  return max(process.extractOne(sigle,x)[1],process.extractOne(f'{sigle} ',x)[1],process.extractOne(f' {sigle}',x)[1], fuzz.partial_ratio(sigle,x))

# Regroup function
def regroup(df,col):
    df_regroup = df.groupby('dc:identifiers')[col].apply(list).reset_index(name=f'regroup_{col}')
    df_regroup[f'regroup_{col}'] = df_regroup[f'regroup_{col}'].apply('|'.join)
    df = pd.merge(df,df_regroup, left_on='dc:identifiers', right_on='dc:identifiers')
    return df