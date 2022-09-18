import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String
import pandas as pd
import numpy as np
import glob
import helpers.crossref_harvest as crf
import helpers.dissemin_harvest as dsm

@asset(config_schema={"model_output_data_path": str, "observation_date": str})
def get_publis_uniques_doi_oa_data_with_bsoclasses(context):
    publis_uniques_doi_oa_data_with_bsoclasses = pd.read_csv(f'{context.op_config["model_output_data_path"]}/{context.op_config["observation_date"]}/publis_uniques_doi_oa_data_with_bsoclasses.csv', sep=",", encoding="utf-8")
    return publis_uniques_doi_oa_data_with_bsoclasses

@asset(config_schema={"intermediate_data_path": str})
def get_crossref_data(context,publis_uniques_doi_oa_data_with_bsoclasses):
    l = publis_uniques_doi_oa_data_with_bsoclasses["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        #print("DOI traités par crossref : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        crf.crf_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv(f'{context.op_config["intermediate_data_path"]}/temp_crf/crf_'+str(i)+".csv",index = False,encoding='utf8')
    # concaténation des csv
    all_filenames = [i for i in glob.glob(f'{context.op_config["intermediate_data_path"]}/temp_crf/*.csv')]
    combined_crf = pd.concat([pd.read_csv(f) for f in all_filenames ])
    crossref_data = combined_crf[combined_crf.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    crossref_data.to_csv(f'{context.op_config["intermediate_data_path"]}/crossref_data.csv',index = False,encoding='utf8')
    return crossref_data

@asset(config_schema={"intermediate_data_path": str})
def get_dissemin_data(context,publis_uniques_doi_oa_data_with_bsoclasses):
    l = publis_uniques_doi_oa_data_with_bsoclasses["doi"].to_list()
    n = 10
    for i in range(0, len(l), n):
        #print("DOI traités par dissemin : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        dsm.dsm_retrieval(l[i:i+n]).to_csv(f'{context.op_config["intermediate_data_path"]}/temp_dsm/dsm_'+str(i)+'.csv',index = False,encoding='utf8')
    #concaténation des cvs
    all_filenames = [i for i in glob.glob(f'{context.op_config["intermediate_data_path"]}/temp_dsm/*.csv')]
    combined_dsm = pd.concat([pd.read_csv(f) for f in all_filenames ])
    dissemin_data = combined_dsm[combined_dsm.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    dissemin_data.to_csv(f'{context.op_config["intermediate_data_path"]}/dissemin_data.csv',index = False,encoding='utf8')
    return dissemin_data

@asset(config_schema={"reporting_data_path": str, "observation_date": str})
def merge_data(context,publis_uniques_doi_oa_data_with_bsoclasses,crossref_data, dissemin_data):
    # crossref data
    publis_uniques_doi_oa_data_with_bsoclasses_complete = publis_uniques_doi_oa_data_with_bsoclasses.merge(crossref_data, left_on='doi', right_on='source_doi',how='left').drop(columns=['source_doi','published-online-date','journal-published-print-date','published-print-date'])
    # dissemin data
    publis_uniques_doi_oa_data_with_bsoclasses_complete = pd.merge(publis_uniques_doi_oa_data_with_bsoclasses_complete,dissemin_data, left_on='doi', right_on='source_doi',how="left").drop(columns=['source_doi'])
    publis_uniques_doi_oa_data_with_bsoclasses_complete.to_csv(f'{context.op_config["reporting_data_path"]}/{context.op_config["observation_date"]}/open_access/publis_uniques_doi_oa_data_with_bsoclasses_complete.csv', index= False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="shape_final_dataset", metadata={
            "text_metadata": 'Number of unique publis with doi and oa metadata',
            "size": f'nb lignes {publis_uniques_doi_oa_data_with_bsoclasses_complete.shape[0]}, nb cols {publis_uniques_doi_oa_data_with_bsoclasses_complete.shape[1]}'})
        )
    return publis_uniques_doi_oa_data_with_bsoclasses_complete

@job
def add_crossref_dissemin():
    #configs
    publis_uniques_doi_oa_data_with_bsoclasses =get_publis_uniques_doi_oa_data_with_bsoclasses()
    crossref_data = get_crossref_data(publis_uniques_doi_oa_data_with_bsoclasses)
    dissemin_data = get_dissemin_data(publis_uniques_doi_oa_data_with_bsoclasses)
    merge_data(publis_uniques_doi_oa_data_with_bsoclasses,crossref_data, dissemin_data)


@repository
def prod_bso_publis_scopus():
    return [add_crossref_dissemin]

"""
Config
ops:
  get_publis_uniques_doi_oa_data_with_bsoclasses:
    config:
      model_output_data_path: bso_publis_scopus/07_model_output
      observation_date: 2022-08-29
  get_crossref_data:
    config:
      intermediate_data_path: bso_publis_scopus/02_intermediate
  get_dissemin_data:
    config:
      intermediate_data_path: bso_publis_scopus/02_intermediate
  merge_data:
    config:
      reporting_data_path: bso_publis_scopus/08_reporting
      observation_date: 2022-08-29
"""


