# Ensemble de fonctions pour requêter les différentes API Scopus et parser les résultats #
import numpy as np
import pandas as pd
import requests
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

scopus_search_url = "https://api.elsevier.com/content/search/scopus"
scopus_abstract_retrieval_url = "https://api.elsevier.com/content/abstract"
api_key1 = "c6ccce863c56222e5b69d56efbb71ea6"
api_key2 = "d9d5d38c677625706a452b4ecb5776e0"
api_key3 = "efd697f6a6a171c01d8c53bb3296ca3a"
api_key4 = "3fc0532caa3be935768d68c1cfed44ad"

def create_pairs_afid_eid(aff_internal_id,aff_scopus_id,record):
    return {
        "aff_internal_id": f"{aff_internal_id}",
        "aff_scopus_id": f"{aff_scopus_id}",
        "dc:identifiers": record["dc:identifier"],
    }

# couple af-id/scopus_id
# pour les structures présentes dans la hiérarchie Scopus
def get_list_publis_ids_by_affid(aff_internal_id,aff_scopus_id,start_year="2016", end_year="2022",count=10,api_key=api_key1):
    global_list = []
    url = f"{scopus_search_url}?query=af-id({aff_scopus_id})&date={start_year}-{end_year}&count=1&field=dc:identifier"
    headers = {
            "X-ELS-APIKey"  : api_key,
            "Accept"        : 'application/json'
            }
    response = requests.request("GET", url, headers=headers).text
    data = json.loads(response)
    total_results = data["search-results"]["opensearch:totalResults"]
    for i in range(0, int(total_results), int(count)):
        url = f"{scopus_search_url}?query=af-id({aff_scopus_id})&date={start_year}-{end_year}&count={count}&start={i}&field=dc:identifier"
        response = requests.request("GET", url, headers=headers).text
        data = json.loads(response)
        for record in data["search-results"]["entry"]:
            result = create_pairs_afid_eid(aff_internal_id,aff_scopus_id,record)
            global_list.append(result)
    return pd.DataFrame(global_list)

# couple af-id/scopus_id
# pour les structures non présentes dans la hiérarchie Scopus (scopus_id de type temp_), avec en plus le param 60110693 (UCA) pour être sûr de choper les publis de labo UCA
def get_list_publis_ids_by_afforg(aff_internal_id,aff_scopus_id,affil_org,start_year="2016", end_year="2022",count=10,api_key=api_key1):
    global_list = []
    url = f"{scopus_search_url}?query=af-id(60110693) AND affilorg({affil_org})&date={start_year}-{end_year}&count=1&field=dc:identifier"
    headers = {
            "X-ELS-APIKey"  : api_key,
            "Accept"        : 'application/json'
            }
    response = requests.request("GET", url, headers=headers).text
    data = json.loads(response)
    total_results = data["search-results"]["opensearch:totalResults"]
    for i in range(0, int(total_results), int(count)):
        url = f"{scopus_search_url}?query=af-id(60110693) AND affilorg({affil_org})&date={start_year}-{end_year}&count={count}&start={i}&field=dc:identifier"
        response = requests.request("GET", url, headers=headers).text
        data = json.loads(response)
        for record in data["search-results"]["entry"]:
            result = create_pairs_afid_eid(aff_internal_id,aff_scopus_id,record)
            global_list.append(result)
    return pd.DataFrame(global_list)

# couple af-id/scopus_id
# Respectivement pour UCA et OCA mentionnées seules (sans labo)
def get_list_publis_ids_by_affid_sanslabo(df_affil_id, df_affil_org,aff_internal_id,aff_scopus_id,start_year="2016", end_year="2022",count=10,api_key=api_key2):
    global_list = []
    query_part_exclude_affid = " AND NOT ".join([f"af-id({s})" for s in df_affil_id.aff_scopus_id.to_list()])
    query_part_exclude_afforg = " AND NOT ".join([f"affilorg({s})" for s in df_affil_org.acronyme.to_list()])
    url = f"{scopus_search_url}?query=af-id({aff_scopus_id}) AND NOT {query_part_exclude_affid} AND NOT {query_part_exclude_afforg}&date={start_year}-{end_year}&count=1&field=dc:identifier"
    headers = {
            "X-ELS-APIKey"  : api_key,
            "Accept"        : 'application/json'
            }
    response = requests.request("GET", url, headers=headers).text
    data = json.loads(response)
    total_results = data["search-results"]["opensearch:totalResults"]
    for i in range(0, int(total_results), int(count)):
        url = f"{scopus_search_url}?query=af-id({aff_scopus_id}) AND NOT {query_part_exclude_affid} AND NOT {query_part_exclude_afforg}&date={start_year}-{end_year}&count={count}&start={i}&field=dc:identifier"
        response = requests.request("GET", url, headers=headers).text
        data = json.loads(response)
        for record in data["search-results"]["entry"]:
            result = create_pairs_afid_eid(aff_internal_id,aff_scopus_id,record)
            global_list.append(result)
    return pd.DataFrame(global_list)

# couple af-id/scopus_id
# pour les scopus ID UCA et OCA mentionnées ensemble (co-publi) mais sans labo
def get_list_publis_ids_by_affid_sanslabo_together(df_affil_id, df_affil_org, start_year="2016", end_year="2022",count=10,api_key=api_key2):
    global_list = []
    query_part_exclude_affid = " AND NOT ".join([f"af-id({s})" for s in df_affil_id.aff_scopus_id.to_list()])
    query_part_exclude_afforg = " AND NOT ".join([f"affilorg({s})" for s in df_affil_org.acronyme.to_list()])
    url = f"{scopus_search_url}?query=af-id(60110693) AND af-id(60010513) AND NOT {query_part_exclude_affid} AND NOT {query_part_exclude_afforg}&date={start_year}-{end_year}&count=1&field=dc:identifier"
    headers = {
            "X-ELS-APIKey"  : api_key,
            "Accept"        : 'application/json'
            }
    response = requests.request("GET", url, headers=headers).text
    data = json.loads(response)
    total_results = data["search-results"]["opensearch:totalResults"]
    for i in range(0, int(total_results), int(count)):
        url = f"{scopus_search_url}?query=af-id(60110693) AND af-id(60010513) AND NOT {query_part_exclude_affid} AND NOT {query_part_exclude_afforg}&date={start_year}-{end_year}&count={count}&start={i}&field=dc:identifier"
        response = requests.request("GET", url, headers=headers).text
        data = json.loads(response)
        for record in data["search-results"]["entry"]:
            result_uca = create_pairs_afid_eid("1","60110693",record)
            result_oca = create_pairs_afid_eid("2","60010513",record)
            global_list.extend((result_uca, result_oca))
    return pd.DataFrame(global_list)
    