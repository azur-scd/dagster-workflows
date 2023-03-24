# Ensemble de fonctions pour requêter les différentes API Scopus et parser les résultats #
import contextlib
import numpy as np
import pandas as pd
import requests
import json
import helpers.functions as fn
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

doaj_base_url = "https://doaj.org/api/search/journals/issn:"

def doaj_metadata(issn_l):
    """
    It takes a list of ISSNs, and for each ISSN, it checks if the ISSN is in the DOAJ API. If it is, it
    returns the metadata for that ISSN. If it isn't, it returns an empty dataframe
    
    :param issn_l: the ISSN of the journal you want to get the metadata for
    :return: A dataframe with the APC metadata from the DOAJ API, colums are has_apc, url, max_0_currency, max_0_price, issn_l
    """    
    if issn_l is None:
        raise ValueError('ISSN cannot be None')
    df_temp = pd.DataFrame()
    with contextlib.suppress(requests.exceptions.RequestException):
        requests.get(doaj_base_url+str(issn_l))
        if requests.get(doaj_base_url+str(issn_l)).status_code == 200:
            response = requests.get(doaj_base_url+str(issn_l)).text
            print(json.loads(response)["total"])
            if json.loads(response)["total"] == 1:
                result = fn.flatten_json(json.loads(response)["results"][0]["bibjson"]["apc"])
                result["issn_l"] = issn_l
                df_temp = df_temp.append([result])
    return df_temp

def doaj_retrieval(issn_list):
    """
    It takes a list of ISSNs, and returns a dataframe of metadata for each ISSN
    
    :param issn_list: a list of ISSNs to query
    :return: A dataframe with the metadata of the journals in the issn_list
    """
    processes = []
    df_collection = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        processes = {executor.submit(doaj_metadata, issn): str(issn) for issn in issn_list}
    for task in as_completed(processes):
        worker_result = task.result()
        df_collection.append(worker_result)
    return pd.concat(df_collection)
        