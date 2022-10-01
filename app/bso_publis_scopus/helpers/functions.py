import numpy as np
import pandas as pd
import re
import texthero as hero
from thefuzz import fuzz
from thefuzz import process
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk import bigrams
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer

# Divers

def keep_duplicate (row):
    return row["author_name"] +"|" + row["affiliation_name"]

# NLP Functions
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

def to_bso_class_with_ml(row,ml_model):
    bso_classes = {0: 'Biology (fond.)', 1: 'Chemistry', 2: 'Computer and \n information sciences', 3: 'Earth, Ecology, \nEnergy and applied biology', 4: 'Engineering',5: 'Humanities', 6: 'Mathematics', 7: 'Medical research', 8: 'Physical sciences, Astronomy', 9: 'Social sciences'} 
    predicted_class = ml_model.predict([[row]])
    return bso_classes[predicted_class[0]]

def str2list(s):
    l = s.split(",")
    return [pd.Series([sub]).apply(remove_stopwords_fr).pipe(hero.clean) for sub in l]

def list2str(l):
    return [','.join(sub) for sub in l]

# Fuzzy extractor v1
def fuzzy_extractone_uca_developpee(l):
    return max(process.extractOne("universite cote azur",l)[1],process.extractOne("university cote azur",l)[1], process.extractOne("azur university",l)[1],process.extractOne("univ cote azur",l)[1],process.extractOne("universite cote 'azur",l)[1],process.extractOne("university cote 'azur",l)[1],process.extractOne("univ cote 'azur",l)[1])
def fuzzy_extractone_uca_sigle(l):
    return process.extractOne("uca",  l)[1]
def fuzzy_extractone_uns_developpee(l):
    return max(process.extractOne("universite nice",l)[1],process.extractOne("universite nice sophia",l)[1],process.extractOne("university nice",l)[1],process.extractOne("university nice sophia",l)[1],process.extractOne("antipolis university",l)[1],process.extractOne("univ nice",l)[1])
def fuzzy_extractone_uns_sigle(l):
    return process.extractOne("uns",l)[1]

# Fuzzy extractor v2
def fuzzy_uca_developpee(l):
    l = [s.replace("'","") for s in l]
    return max(process.extractOne("universite cote azur",l)[1],
               process.extractOne("university cote azur",l)[1], 
               process.extractOne("universite azur",l)[1],
               process.extractOne("cote azur university",l)[1], 
               fuzz.partial_ratio("univ cote azur",l),
               process.extractOne("universitcrossed sign cote azur",l)[1],
               process.extractOne("universitcrossed cote azur",l)[1],
               process.extractOne("universit x00e9 c x00f4 te x2019 azur",l)[1])
def fuzzy_uca_sigle(l):
    return max(process.extractOne("uca",  l)[1],fuzz.partial_ratio("uca",l)) 
def fuzzy_uns(l):
    return max(process.extractOne("universite nice",l)[1],
               process.extractOne("universite nice sophia",l)[1],
               process.extractOne("nice sophia antipolis",l)[1],
               process.extractOne("universite sophia antipolis",l)[1],
               process.extractOne("university nice",l)[1],
               process.extractOne("university nice sophia",l)[1],
               process.extractOne("university sophia antipolis",l)[1],
               process.extractOne("sophia antipolis university",l)[1],
               process.extractOne("antipolis university",l)[1],
               process.extractOne("nice university",l)[1],
               process.extractOne("univ nice",l)[1],
               fuzz.partial_ratio("univ nice",l),
               fuzz.partial_ratio("uns",l))

# Regroup function
def regroup(df,col):
    df_regroup = df.groupby('dc:identifiers')[col].apply(list).reset_index(name=f'regroup_{col}')
    df_regroup[f'regroup_{col}'] = df_regroup[f'regroup_{col}'].apply('|'.join)
    df = pd.merge(df,df_regroup, left_on='dc:identifiers', right_on='dc:identifiers')
    return df
