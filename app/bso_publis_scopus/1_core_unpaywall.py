import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String
import pandas as pd
import numpy as np
import requests
import json
import glob
import os
import helpers.unpaywall_harvest as upw
import helpers.crossref_harvest as crf
import helpers.dissemin_harvest as dsm
import helpers.bso_classification_harvest as bso
import helpers.functions as fn
from joblib import dump, load

@op(config_schema={"intermediate_data_path": str})
def create_temp_subfolders(context):
    enrichments_list = ["upw","crf","dsm"]
    for i in enrichments_list:
        if not os.path.exists(f'{context.op_config["intermediate_data_path"]}/temp_{i}'):
            os.mkdir(f'{context.op_config["intermediate_data_path"]}/temp_{i}')  

@op(config_schema={"primary_data_path": str, "model_output_data_path": str, "reporting_data_path": str,"observation_date": str})
def create_data_observation_subfolders(context):
    concerned_folders = ["primary_data_path","model_output_data_path","reporting_data_path"]
    for i in concerned_folders:
        if not os.path.exists(f'{context.op_config[i]}/{context.op_config["observation_date"]}'):
            os.mkdir(f'{context.op_config[i]}/{context.op_config["observation_date"]}')
            if i == "primary_data_path":
                os.mkdir(f'{context.op_config["primary_data_path"]}/{context.op_config["observation_date"]}/publis_non_traitees')

@asset(config_schema={"raw_data_path": str, "observation_date": str})
def extract_data_source(context):
    df = pd.read_json(f'{context.op_config["raw_data_path"]}/{context.op_config["observation_date"]}/exportDonnees_barometre_complet_{context.op_config["observation_date"]}.json')
    context.log_event(
        AssetObservation(asset_key="initial_jp_dataset", metadata={
            "text_metadata": 'Shape of the initial JP dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    # keep columns
    df = df[["dc:identifiers","prism:doi","reference","annee_pub","@afids","mentionAffil_reconstruct","@auid","ce:indexed-name","corresponding_author","Is_dc:creator"]]
    # rename columns
    df.columns = ['source_id', 'doi',"scopus_title",'year', 'aff_scopus_id','aff_source_text','author_id','author_name','corresponding_author','creator_author']
    # concatene authors
    df_authors = df.groupby('source_id')['author_name'].apply(list).reset_index(name='all_authors')
    df_authors['all_authors'] = df_authors["all_authors"].apply('|'.join)
    df_reference_data = pd.merge(df,df_authors, left_on='source_id', right_on='source_id')
    return df_reference_data

@asset(config_schema={"primary_data_path": str})
def update_referentiel_data(context,df_reference_data):
    # fetch live data
    df_affiliations = pd.read_json(f'{context.op_config["primary_data_path"]}/referentiel_structures.json')
    # rename and save the file in his last state
    df_affiliations.to_json(f'{context.op_config["primary_data_path"]}/referentiel_structures_old.json',orient="records",indent=3,force_ascii=False)
    context.log_event(
        AssetObservation(asset_key="last_affiliations_dataset", metadata={
            "text_metadata": 'Shape of the last affiliations dataset',
            "size": f"nb lignes {df_affiliations.shape[0]}, nb cols {df_affiliations.shape[1]}"})
    )
    # LA MAJ PREALABLE AVEC LES NOUVELLES STRUCTURES SE FAIT DANS LE NOTEBOOK DE CONTROLE
    # update records sums columns
    #affiliations =affiliations.drop(['document-count-period'])
    df_count = df_reference_data.groupby("aff_scopus_id")['doi'].nunique().reset_index().rename(columns={'doi':'counts'}).convert_dtypes()
    df_affiliations = pd.merge(df_affiliations,df_count, left_on='affiliation_id', right_on='aff_scopus_id',how="left").drop(columns=['aff_scopus_id','documents_count']).rename(columns={'counts':'documents_count'})
    df_affiliations['documents_count'] = df_affiliations['documents_count'].fillna(0)
    df_affiliations.to_json(f'{context.op_config["primary_data_path"]}/referentiel_structures.json',orient="records",indent=3,force_ascii=False)
    context.log_event(
        AssetObservation(asset_key="new_affiliations_dataset", metadata={
            "text_metadata": 'Shape of the new affiliations dataset',
            "size": f"nb lignes {df_affiliations.shape[0]}, nb cols {df_affiliations.shape[1]}"})
        )
    return df_affiliations

@asset(config_schema={"reporting_data_path": str, "observation_date": str})
def get_publis_all_with_affiliations_data(context,df_reference_data,df_affiliations):
    # merge all publis with affiliations
    df_affiliations["affiliation_id"] = df_affiliations["affiliation_id"].astype('str')
    df_reference_data["aff_scopus_id"] = df_reference_data["aff_scopus_id"].astype('str')
    publis_all_with_affiliations_data = pd.merge(df_reference_data,df_affiliations[df_affiliations["affiliation_id"].notna()], left_on='aff_scopus_id', right_on='affiliation_id',how="left").drop(columns=['affiliation_id','documents_count','ppn_valide','affcourt_valide','RNSR','VIAF','ISNI','BNF','HAL'])
    publis_all_with_affiliations_data = publis_all_with_affiliations_data.rename(columns={'id': 'aff_internal_id', 'parent_id': 'aff_parent_id'})
    # identify corresponding author if UCA
    publis_all_with_affiliations_data["corresponding"] = publis_all_with_affiliations_data[publis_all_with_affiliations_data["corresponding_author"] == "oui"].apply (lambda row: fn.keep_duplicate(row), axis=1)
    publis_all_with_affiliations_data.to_csv(f'{context.op_config["reporting_data_path"]}/{context.op_config["observation_date"]}/publis_all_with_affiliations_data.csv',index = False,encoding='utf8')
    return publis_all_with_affiliations_data

@asset(config_schema={"intermediate_data_path": str, "corpus_end_year": int})
def get_publis_uniques_doi_data(context,publis_all_with_affiliations_data,):
    # Deduplicate
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].astype('category')
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].cat.set_categories(['oui', 'non', 'corr absent pour cette publi'], ordered=True)
    publis_all_with_affiliations_data.sort_values(by=['doi', 'corresponding_author'])
    publis_uniques_doi_data = publis_all_with_affiliations_data[publis_all_with_affiliations_data.doi.notna()].drop_duplicates(subset=['doi'], keep='first')[["source_id","doi","year","corresponding","all_authors"]]
    publis_uniques_doi_data = publis_uniques_doi_data[publis_uniques_doi_data.year < context.op_config["corpus_end_year"]]
    publis_uniques_doi_data.to_csv(f'{context.op_config["intermediate_data_path"]}/publis_uniques_doi_data.csv',index = False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="unique_uca_doi", metadata={
            "text_metadata": 'Number of unique publis with doi',
            "size": f'nb lignes {publis_uniques_doi_data["doi"].isna().shape[0]}, nb cols {publis_uniques_doi_data.shape[1]}'})
        )
    return publis_uniques_doi_data

@asset(config_schema={"primary_data_path": str})
def get_publishers_doi_prefix(context):
    # LA MAJ PREALABLE AVEC LES NOUVEAUX PREFIXES SE FAIT DANS LE NOTEBOOK DE CONTROLE
    publishers_doi_prefix = pd.read_csv(f'{context.op_config["primary_data_path"]}/mapping_doiprefixes_publisher.csv', sep=",",encoding='utf8').drop_duplicates(subset=['prefix'], keep='last')
    context.log_event(
        AssetObservation(asset_key="new_publisher_doiprefix", metadata={
            "text_metadata": 'Number of new publishers doi prefix',
            "size": f'nb lignes {publishers_doi_prefix.shape[0]}'})
        )
    publishers_doi_prefix.to_csv(f'{context.op_config["primary_data_path"]}/mapping_doiprefixes_publisher.csv', index = False,encoding='utf8')
    return publishers_doi_prefix

@asset(config_schema={"intermediate_data_path": str})
def get_unpaywall_data(context,publis_uniques_doi_data):
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        #print("DOI traités par unpaywall : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        upw.upw_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv(f'{context.op_config["intermediate_data_path"]}/temp_upw/upw_'+str(i)+'.csv',index = False,encoding='utf8')
    #concaténation des cvs
    all_filenames = [i for i in glob.glob(f'{context.op_config["intermediate_data_path"]}/temp_upw/*.csv')]
    unpaywall_data = pd.concat([pd.read_csv(f).drop(columns=['oa_locations']) for f in all_filenames ]).drop_duplicates(subset=['source_doi'], keep='last')
    unpaywall_data.to_csv(f'{context.op_config["intermediate_data_path"]}/unpaywall_data.csv',index = False,encoding='utf8')
    return unpaywall_data

@asset(config_schema={"primary_data_path": str, "observation_date": str})
def merge_upw_data(context,publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data):
    # unpaywall data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_data,unpaywall_data, left_on='doi', right_on='source_doi',how="right").drop(columns=['source_doi','year_upw'])
    # publishers doi prefix
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data.apply (lambda row: str(row["doi"].partition("/")[0]), axis=1) 
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data["doi_prefix"].astype(str)
    publishers_doi_prefix["prefix"] = publishers_doi_prefix["prefix"].astype(str)
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(publishers_doi_prefix, left_on='doi_prefix', right_on='prefix',how='left').drop(columns=['prefix'])
    publis_uniques_doi_oa_data.to_csv(f'{context.op_config["primary_data_path"]}/{context.op_config["observation_date"]}/publis_uniques_doi_oa_data.csv', index= False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="unique_uca_doi_with_oa", metadata={
            "text_metadata": 'Number of unique publis with doi and oa metadata',
            "size": f'nb lignes {publis_uniques_doi_oa_data.shape[0]}, nb cols {publis_uniques_doi_oa_data.shape[1]}'})
        )
    return publis_uniques_doi_oa_data

@job
def core_unpaywall():
    #configs
    create_temp_subfolders()
    create_data_observation_subfolders()
    data_source = extract_data_source()
    referentiel_data = update_referentiel_data(data_source)
    publis_all_with_affiliations_data = get_publis_all_with_affiliations_data(data_source,referentiel_data)
    #control_missing_doi(publis_all_with_affiliations_data)
    publis_uniques_doi_data = get_publis_uniques_doi_data(publis_all_with_affiliations_data)
    publishers_doi_prefix = get_publishers_doi_prefix()
    unpaywall_data = get_unpaywall_data(publis_uniques_doi_data)
    publis_uniques_doi_oa_data = merge_upw_data(publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data)


@repository
def prod_bso_publis_scopus():
    return [core_unpaywall]

"""
Config
ops:
  create_data_observation_subfolders:
    config:
      model_output_data_path: bso_publis_scopus/07_model_output
      observation_date: 2022-08-29
      primary_data_path: bso_publis_scopus/03_primary
      reporting_data_path: bso_publis_scopus/08_reporting
  create_temp_subfolders:
    config:
      intermediate_data_path: bso_publis_scopus/02_intermediate
  extract_data_source:
    config:
      observation_date: 2022-08-29
      raw_data_path: bso_publis_scopus/01_raw
  get_publis_all_with_affiliations_data:
    config:
      observation_date: 2022-08-29
      reporting_data_path: bso_publis_scopus/08_reporting
  get_publis_uniques_doi_data:
    config:
      corpus_end_year: 2022
      intermediate_data_path: bso_publis_scopus/02_intermediate
  get_publishers_doi_prefix:
    config:
      primary_data_path: bso_publis_scopus/03_primary
  update_referentiel_data:
    config:
      primary_data_path: bso_publis_scopus/03_primary
  get_unpaywall_data:
    config:
      intermediate_data_path: bso_publis_scopus/02_intermediate
  merge_upw_data:
    config:
      observation_date: 2022-08-29
      primary_data_path: bso_publis_scopus/03_primary
"""


