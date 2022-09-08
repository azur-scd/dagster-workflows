import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String
import pandas as pd
import numpy as np
import requests
import json
import glob
import os

@asset(config_schema={"intermediate_data_path": str})
def get_publis_uniques_doi_data(context):
    publis_uniques_doi_data = pd.read_csv(f'{context.op_config["intermediate_data_path"]}/publis_uniques_doi_data.csv', sep=",", encoding="utf-8")
    return publis_uniques_doi_data

@asset(config_schema={"intermediate_data_path": str})
def get_upw_data(context):
    upw_data = pd.read_csv(f'{context.op_config["intermediate_data_path"]}/unpaywall_data.csv', sep=",", encoding="utf-8")
    return upw_data


@asset(config_schema={"primary_data_path": str, "observation_date": str})
def merge_upw_data(context,publis_uniques_doi_data,upw_data):
    # unpaywall data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_data,upw_data, left_on='doi', right_on='source_doi',how="right").drop(columns=['source_doi','year_upw'])
    # publishers doi prefix
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data.apply (lambda row: str(row["doi"].partition("/")[0]), axis=1) 
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data["doi_prefix"].astype(str)
    publishers_doi_prefix = pd.read_csv(f'{context.op_config["primary_data_path"]}/mapping_doiprefixes_publisher.csv', sep=",", encoding="utf-8")
    publishers_doi_prefix["prefix"] = publishers_doi_prefix["prefix"].astype(str)
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(publishers_doi_prefix, left_on='doi_prefix', right_on='prefix',how='left').drop(columns=['prefix'])
    # crossref data
    #publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(crossref_data, left_on='doi', right_on='source_doi',how='left').drop(columns=['source_doi','published-online-date','journal-published-print-date','published-print-date'])
    # dissemin data
    #publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_oa_data,dissemin_data, left_on='doi', right_on='source_doi',how="left").drop(columns=['source_doi'])
    publis_uniques_doi_oa_data.to_csv(f'{context.op_config["primary_data_path"]}/{context.op_config["observation_date"]}/publis_uniques_doi_oa_data.csv', index= False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="unique_uca_doi_with_oa", metadata={
            "text_metadata": 'Number of unique publis with doi and oa metadata',
            "size": f'nb lignes {publis_uniques_doi_oa_data.shape[0]}, nb cols {publis_uniques_doi_oa_data.shape[1]}'})
        )
    return publis_uniques_doi_oa_data

@job
def dev_workflow():
   publis_uniques_doi_data = get_publis_uniques_doi_data()
   upw_data = get_upw_data()
   merge_upw_data(publis_uniques_doi_data,upw_data)


@repository
def dev_bso_publis_scopus():
    return [dev_workflow]

"""
Config
ops:
  get_publis_uniques_doi_data:
    config:
      intermediate_data_path: bso_publis_scopus/02_intermediate
  get_upw_data:
    config:
      intermediate_data_path: bso_publis_scopus/02_intermediate
  merge_upw_data:
    config:
      observation_date: 2022-08-29
      primary_data_path: bso_publis_scopus/03_primary
"""


