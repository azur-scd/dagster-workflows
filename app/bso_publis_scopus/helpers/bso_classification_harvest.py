import numpy as np
import pandas as pd
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
from nltk.corpus import stopwords
from nltk import bigrams
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer

# NLP Functions
stop_en = set(stopwords.words('english')) 

def to_lower(row,col):
    return str.lower(row[f'{col}_s']).strip()

tokenizer = RegexpTokenizer(r'\w+')
def tokenize(row,col):
    return [token for token in tokenizer.tokenize(row[f'{col}_lower_stop_words'].strip()) if ((token != u"") & (len(token)>2))]

lemmatizer = WordNetLemmatizer()
def lemmatize(row,col):
    return [lemmatizer.lemmatize(word) for word in row[f'{col}_lower_stop_words_token']]

def list_to_string(row,col):
    return " ".join([s for s in row[f'{col}_lower_stop_words_token_lemme']])

def text_process(df,col):
    df[f"{col}_lower"] = df.apply(lambda row: to_lower(row,col),axis=1)
    df[f'{col}_lower_stop_words'] = df[f'{col}_lower'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_en)]))
    df[f'{col}_lower_stop_words_token'] = df.apply(lambda row: tokenize(row,col),axis=1)
    df[f'{col}_lower_stop_words_token_lemme'] = df.apply(lambda row: lemmatize(row,col),axis=1)
    df[f'{col}_cleaned'] = df.apply(lambda row: list_to_string(row,col), axis=1)
    return df

def to_bso_class_with_ml(row,bso_classes,model):
    predicted_class = model([row["features_union_titre_journal"]]) 
    return bso_classes[predicted_class[0]]