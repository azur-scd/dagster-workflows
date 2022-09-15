import numpy as np
import pandas as pd
import re
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk import bigrams
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer

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