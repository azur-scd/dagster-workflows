import numpy as np
import pandas as pd

def keep_duplicate (row):
    return row["author_name"] +"|" + row["affiliation_name"]

def flatten_json(y):
    """
    It takes a nested dictionary and returns a flattened dictionary
    
    :param y: The nested JSON object that you want to flatten
    :return: A dictionary with the flattened keys and values.
    """
    out = {}
 
    def flatten(x, name=''):
 
        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:
 
            for a in x:
                flatten(x[a], name + a + '_')
 
        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:
 
            i = 0
 
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
 
    flatten(y)
    return out
