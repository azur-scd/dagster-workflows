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

@op(config_schema={"raw_data_path": str,"intermediate_data_path": str, "primary_data_path": str, "model_input_data_path": str, "models_data_path": str, "model_output_data_path": str, "observation_date": str, "corpus_end_year": str, "bso_classes": dict, "ml_model_path": str})
def get_config(context):
    return context.op_config

@op
def create_temp_subfolders(config_params):
    enrichments_list = ["upw","crf","dsm"]
    for i in enrichments_list:
        if not os.path.exists(f'{config_params["intermediate_data_path"]}/{i}'):
            os.mkdir(f'{config_params["intermediate_data_path"]}/{i}')  

@op
def create_data_observation_subfolders(config_params):
    concerned_folders = ["primary_data_path","model_output_data_path"]
    for i in concerned_folders:
        if not os.path.exists(f'{config_params[i]}/{config_params["observation_date"]}'):
            os.mkdir(f'{config_params[i]}/{config_params["observation_date"]}')
    #pour les publis non traitées car sans doi
    os.mkdir(f'{config_params["primary_data_path"]}/{config_params["observation_date"]}/publis_non_traitees')

@asset
def get_mesri_bso_dataset():
    df_bso = pd.read_csv('https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.csv.gz',compression='gzip',encoding='utf8').convert_dtypes()
    return df_bso

@asset
def extract_source_data(context,config_params):
    df = pd.read_json(f'{config_params["raw_data_path"]}/{config_params["observation_date"]}/exportDonnees_barometre_complet_{config_params["observation_date"]}.json')
    context.log_event(
        AssetObservation(asset_key="initial__dataset", metadata={
            "text_metadata": 'Shape of the initial dataset',
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

@asset
def update_referentiel_data(df,config_params):
    # fetch live data
    df_affiliations = pd.read_json(f'{config_params["primary_data_path"]}/referentiel_structures.json')
    # rename and save the file in his last state
    df_affiliations.to_json(f'{config_params["primary_data_path"]}/referentiel_structures_old.json',orient="records",indent=3,force_ascii=False)
    # compare with current data df to see if new affiliations must be added ton the referential
    set_affiliations_id = set(df_affiliations['affiliation_id'].tolist())
    set_data_affiliations_id = set(df['aff_scopus_id'].unique().tolist())
    diff = set_affiliations_id.union(set_data_affiliations_id)  - set_affiliations_id.intersection(set_data_affiliations_id) 
    if len(diff) == 1:
        # update records sums columns
        #affiliations =affiliations.drop(['document-count-period'])
        df_count = df.groupby("aff_internal_id")['doi'].nunique().reset_index().rename(columns={'doi':'counts'}).convert_dtypes()
        df_affiliations = pd.merge(df_affiliations,df_count, left_on='id', right_on='aff_internal_id',how="left").drop(columns=['aff_internal_id','documents_count']).rename(columns={'counts':'documents_count'})
        df_affiliations['documents_count'] = df_affiliations['documents_count'].fillna(0)
        df_affiliations.to_json(f'{config_params["primary_data_path"]}/referentiel_structures.json',orient="records",indent=3,force_ascii=False)
        return df_affiliations
    #else: gestion erreur

@asset
def get_publis_with_affiliations_data(df_reference_data,df_affiliations,config_params):
    # merge all publis with affiliations
    df_affiliations["affiliation_id"] = df_affiliations["affiliation_id"].astype('str')
    df_reference_data["aff_scopus_id"] = df_reference_data["aff_scopus_id"].astype('str')
    publis_all_with_affiliations_data = pd.merge(df_reference_data,df_affiliations[df_affiliations["affiliation_id"].notna()], left_on='aff_scopus_id', right_on='affiliation_id',how="left").drop(columns=['affiliation_id','documents_count','ppn_valide','affcourt_valide','RNSR','VIAF','ISNI','BNF','HAL'])
    publis_all_with_affiliations_data = publis_all_with_affiliations_data.rename(columns={'id': 'aff_internal_id', 'parent_id': 'aff_parent_id'})
    # identify corresponding author if UCA
    publis_all_with_affiliations_data["corresponding"] = publis_all_with_affiliations_data[publis_all_with_affiliations_data["corresponding_author"] == "oui"].apply (lambda row: fn.keep_duplicate(row), axis=1)
    publis_all_with_affiliations_data.to_csv(f'{config_params["primary_data_path"]}/{config_params["observation_date"]}/publis_all_with_affiliations_data.csv',index = False,encoding='utf8')
    return publis_all_with_affiliations_data

@asset
def control_missing_doi(publis_all_with_affiliations_data,config_params):
    # % missing DOI
    df_unique = publis_all_with_affiliations_data.drop_duplicates(subset=['source_id'], keep='last')
    # save missing doi publs
    df_unique["doi"].isna().to_csv(f'{config_params["primary_data_path"]}/{config_params["observation_date"]}/publis_non_traitees/without_doi.csv',index = False,encoding='utf8')

@asset
def get_publis_uniques_doi_data(publis_all_with_affiliations_data,config_params):
    # Deduplicate
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].astype('category')
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].cat.set_categories(['oui', 'non', 'corr absent pour cette publi'], ordered=True)
    publis_all_with_affiliations_data.sort_values(by=['doi', 'corresponding_author'])
    publis_uniques_doi_data = publis_all_with_affiliations_data[publis_all_with_affiliations_data.doi.notna()].drop_duplicates(subset=['doi'], keep='first')[["source_id","doi","year","corresponding","all_authors"]]
    publis_uniques_doi_data = publis_uniques_doi_data[publis_uniques_doi_data.year < config_params["corpus_end_year"]]
    publis_uniques_doi_data.to_csv(f'{config_params["intermediate_data_path"]}/publis_uniques_doi_data.csv',index = False,encoding='utf8')
    return publis_uniques_doi_data

@asset
def update_publiher_doiprefix_data(publis_uniques_doi_data,config_params):
    new_prefix_list = list(set([item.partition("/")[0] for item in publis_uniques_doi_data["doi"].to_list()]))
    old_prefix_df = pd.read_csv(f'{config_params["primary_data_path"]}/mapping_doiprefixes_publisher.csv', sep=",",encoding='utf8')
    old_prefix_list = old_prefix_df["prefix"].astype(str).to_list()
    diff_prefix_list = list(set(new_prefix_list) - set(old_prefix_list))
    print("Nombre de préfixes à ajouter : " + str(len(diff_prefix_list)))
    df_new_prefix_result = crf.crf_publisher_retrieval(diff_prefix_list)
    publishers_doi_prefix = old_prefix_df.append(df_new_prefix_result)
    publishers_doi_prefix.drop_duplicates(subset=['prefix'], keep='last').to_csv(f'{config_params["primary_data_path"]}/mapping_doiprefixes_publisher.csv', index = False,encoding='utf8')
    return publishers_doi_prefix

@asset
def get_unpaywall_data(publis_uniques_doi_data,config_params):
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        #print("DOI traités par unpaywall : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        upw.upw_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv(f'{config_params["intermediate_data_path"]}/temp_upw/upw_'+str(i)+'.csv',index = False,encoding='utf8')
    #concaténation des cvs
    all_filenames = [i for i in glob.glob(f'{config_params["intermediate_data_path"]}/temp_upw/*.csv')]
    unpaywall_data = pd.concat([pd.read_csv(f).drop(columns=['oa_locations']) for f in all_filenames ]).drop_duplicates(subset=['source_doi'], keep='last')
    unpaywall_data.to_csv(f'{config_params["intermediate_data_path"]}/unpaywall_data.csv',index = False,encoding='utf8')
    return unpaywall_data

@asset
def get_crossref_data(publis_uniques_doi_data,config_params):
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        #print("DOI traités par crossref : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        crf.crf_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv("data/02_intermediate/temp_crf/crf_"+str(i)+".csv",index = False,encoding='utf8')
    # concaténation des csv
    all_filenames = [i for i in glob.glob(f'{config_params["intermediate_data_path"]}/temp_crf/*.csv')]
    combined_crf = pd.concat([pd.read_csv(f) for f in all_filenames ])
    crossref_data = combined_crf[combined_crf.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    crossref_data.to_csv(f'{config_params["intermediate_data_path"]}/crossref_data.csv',index = False,encoding='utf8')
    return crossref_data

@asset
def get_dissemin_data(publis_uniques_doi_data,config_params):
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 10
    for i in range(0, len(l), n):
        #print("DOI traités par dissemin : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        dsm.dsm_retrieval(l[i:i+n]).to_csv('{config_params["intermediate_data_path"]}/temp_dsm/dsm_'+str(i)+'.csv',index = False,encoding='utf8')
    #concaténation des cvs
    all_filenames = [i for i in glob.glob('data/02_intermediate/temp_dsm/*.csv')]
    combined_dsm = pd.concat([pd.read_csv(f) for f in all_filenames ])
    dissemin_data = combined_dsm[combined_dsm.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    dissemin_data.to_csv(f'{config_params["intermediate_data_path"]}/dissemin_data.csv',index = False,encoding='utf8')
    return dissemin_data

@graph
def merge_all_data(publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data,crossref_data,dissemin_data,config_params):
    # unpaywall data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_data,unpaywall_data, left_on='doi', right_on='source_doi',how="right").drop(columns=['source_doi','year_upw'])
    # publishers doi prefix
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data.apply (lambda row: str(row["doi"].partition("/")[0]), axis=1) 
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data["doi_prefix"].astype(str)
    publishers_doi_prefix["prefix"] = publishers_doi_prefix["prefix"].astype(str)
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(publishers_doi_prefix, left_on='doi_prefix', right_on='prefix',how='left').drop(columns=['prefix'])
    # crossref data
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(crossref_data, left_on='doi', right_on='source_doi',how='left').drop(columns=['source_doi','published-online-date','journal-published-print-date','published-print-date'])
    # dissemin data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_oa_data,dissemin_data, left_on='doi', right_on='source_doi',how="left").drop(columns=['source_doi'])
    publis_uniques_doi_oa_data.to_csv(f'{config_params["primary_data_path"]}/{config_params["observation_date"]}/publis_uniques_doi_oa_data.csv', index= False,encoding='utf8')
    return publis_uniques_doi_oa_data

@op
def monitoring_publis_already_classified(context,df_bso,publis_uniques_doi_oa_data):
    list_uca_doi = publis_uniques_doi_oa_data["doi"].tolist()
    context.log_event(
        AssetObservation(asset_key="publis_already_classified", metadata={
            "text_metadata": 'Part des publis UCA présentes dans le dataset du ministère et pour lesquelles on peut déjà récupérer la bso_classification',
            "size": "{}".format(round(len(df_bso[df_bso['doi'].isin(list_uca_doi)]['doi'].tolist()) / df_bso,publis_uniques_doi_oa_data.shape[0] * 100))})
    )

@op
def nlp_transform(df):
    for i in ['title','journal_name','publisher']:
        df[i] = df[i].str.replace('\d+', '')
        df = fn.text_process(df,i)
    return df

@asset
def process_classification(df_bso,publis_uniques_doi_oa_data,config_params):
    #1ère étape : on duplique le dataset oa
    temp = publis_uniques_doi_oa_data[["doi","year","title","journal_name","publisher"]]
    #2ème étape : on ajoute une col 
    temp["bso_classification"] = np.nan
    #3ème étape : on récupère la bso_classification du mesri pour les doi présents
    col = 'doi'
    cols_to_replace = ['bso_classification']
    temp.loc[temp[col].isin(df_bso[col]), cols_to_replace] = df_bso.loc[df_bso[col].isin(temp[col]),cols_to_replace].values
    #4ème étape : préparation NLP sur les publis non classifiées restantes
    df_ml_prepared = bso.nlp_transform(temp[temp.bso_classification.isna() & temp.title.notna() & temp.publisher() & temp.journal_name.notna()])
    df_ml_prepared["features_union"] = df_ml_prepared["title_cleaned"] + ' '  +  df_ml_prepared["journal_name_cleaned"] + ' '  +  df_ml_prepared["publisher_cleaned"] + ' '  +  df_ml_prepared["year"].astype(str)
    #5ème étape : on applique le modèle et on complète le dataste temp
    df_ml_prepared["bso_classification"] = df_ml_prepared.apply(lambda row: bso.to_bso_class_with_ml(row,config_params["bso_classes"],config_params["ml_model_path"]),axis=1)
    temp.loc[temp[col].isin(df_ml_prepared[col]), cols_to_replace] = df_ml_prepared.loc[df_ml_prepared[col].isin(temp[col]),cols_to_replace].values
    temp.to_csv(f'{config_params["model_output_data_path"]}/{config_params["observation_date"]}/uca_doi_classified.csv',index = False,encoding='utf8')
    return temp

@asset
def complete_classification_labels(temp,config_params):
    df_classification_mapping = pd.read_json(f'{config_params["primary_data_path"]}/classification_mapping.json')
    temp_mapped=pd.merge(temp,df_classification_mapping, left_on='bso_classification', right_on='name_en',how="left").drop(columns=['name_en']).rename(columns={"name_fr": "bso_classification_fr"})
    return temp_mapped

@asset
def complete_oa_with_classification(temp_mapped,publis_uniques_doi_oa_data,config_params):
    col = 'doi'
    cols_to_replace = ['bso_classification_fr','main_domain']
    #merge classification with main publis_uniques_doi_oa_data
    for c in cols_to_replace:
        publis_uniques_doi_oa_data[c] = np.nan
        publis_uniques_doi_oa_data.loc[publis_uniques_doi_oa_data[col].isin(temp_mapped[col]), cols_to_replace] = temp_mapped.loc[temp_mapped[col].isin(publis_uniques_doi_oa_data[col]),cols_to_replace].values
        #publications non classifiées en "unknown"
        publis_uniques_doi_oa_data[c] = publis_uniques_doi_oa_data[c].fillna("unknown").replace(r'\r\n', '', regex=True)
    publis_uniques_doi_oa_data.to_csv(f'{config_params["model_output_data_path"]}/{config_params["observation_date"]}/publis_uniques_doi_oa_data_with_bsoclasses.csv', index= False,encoding='utf8')
    return publis_uniques_doi_oa_data 

@job
def run_workflow():
    #configs
    config_params = get_config()
    create_temp_subfolders(config_params)
    create_data_observation_subfolders(config_params)
    get_mesri_bso_dataset()


@repository
def dev_bso_publis_scopus_workflow():
    return [run_workflow]

"""
Config
ops:
  get_config:
     config:
      raw_data_path: bso_publis_scopus/01_raw
      intermediate_data_path: bso_publis_scopus/02_intermediate
      primary_data_path: bso_publis_scopus/03_primary
      model_input_data_path: bso_publis_scopus/05_model_input
      models_data_path: bso_publis_scopus/06_models
      model_output_data_path: bso_publis_scopus/07_model_output
      observation_date: 2022-05-30
      corpus_end_year: "2022"
      ml_model_path: bso_publis_scopus/06_models/logmodel.joblib
      bso_classes: {0: 'Biology (fond.)', 1: 'Chemistry', 2: 'Computer and information sciences', 
               3: 'Earth, Ecology, Energy and applied biology', 4: 'Engineering',
               5: 'Humanities', 6: 'Mathematics', 7: 'Medical research', 
               8: 'Physical sciences, Astronomy', 9: 'Social sciences'}
"""


