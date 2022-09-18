import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String
import pandas as pd
import numpy as np
import requests
import json
import glob
import os
import helpers.functions as fn
import texthero as hero
import joblib
import pickle

@asset
def get_mesri_bso_dataset():
    mesri_bso_dataset = pd.read_csv('https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.csv.gz',compression='gzip',encoding='utf8').convert_dtypes()
    return mesri_bso_dataset

@asset(config_schema={"primary_data_path": str, "observation_date": str})
def get_publis_uniques_doi_oa_data(context):
    publis_uniques_doi_oa_data = pd.read_csv(f'{context.op_config["primary_data_path"]}/{context.op_config["observation_date"]}/publis_uniques_doi_oa_data.csv', sep=",", encoding="utf-8")
    return publis_uniques_doi_oa_data

"""@op(config_schema={"models_path": str})
def get_countvect_model(context):
    countvect_model = pickle.load(open(f'{context.op_config["models_path"]}/count_vect.pkl', 'rb'))
    return countvect_model"""

@op(config_schema={"models_path": str})
def get_logmodel(context):
    logmodel = joblib.load(f'{context.op_config["models_path"]}/logmodel.joblib')
    return logmodel

"""
bso_classes are defined dierctly in the fn.to_bso_class_with_ml function
@op(config_schema={"bso_classes": dict})
def get_bso_classes(context):
    bso_classes = dict(context.op_config["bso_classes"])
    return bso_classes"""

@op
def monitoring_publis_already_classified(context,df_bso,publis_uniques_doi_oa_data):
    list_uca_doi = publis_uniques_doi_oa_data["doi"].tolist()
    context.log_event(
        AssetObservation(asset_key="publis_already_classified", metadata={
            "text_metadata": 'Pourcentage des publis UCA présentes dans le dataset du ministère et pour lesquelles on peut déjà récupérer la bso_classification',
            "size": "{}".format(round(len(df_bso[df_bso['doi'].isin(list_uca_doi)]['doi'].tolist()) / len(list_uca_doi) * 100))})
    )

@asset(config_schema={"model_output_data_path": str, "observation_date": str})
def process_classification(context,mesri_bso_dataset,publis_uniques_doi_oa_data,logmodel):
    #1ère étape : on duplique le dataset oa
    temp = publis_uniques_doi_oa_data[["doi","title","journal_name","publisher"]]
    #2ème étape : on ajoute une col 
    temp["bso_classification"] = np.nan
    #3ème étape : on récupère la bso_classification du mesri pour les doi présents
    col = 'doi'
    cols_to_replace = ['bso_classification']
    temp.loc[temp[col].isin(mesri_bso_dataset[col]), cols_to_replace] = mesri_bso_dataset.loc[mesri_bso_dataset[col].isin(temp[col]),cols_to_replace].values
    #4ème étape : préparation NLP sur les publis non classifiées restantes
    temp1 = temp[(temp.bso_classification.isna()) & (temp.title.notna()) & (temp.publisher.notna()) & (temp.journal_name.notna())]
    temp1["feature"] = temp1["title"] + ' '  +  temp1["journal_name"] + ' '  +  temp1["publisher"]
    temp1['cleaned_feature'] = (
            temp1['feature']
            .pipe(hero.clean)
            .apply(fn.remove_stopwords_fr)
            .apply(fn.remove_special_characters)
            .apply(fn.lemmatize)
    )
    #5ème étape : on applique le modèle et on complète le dataste temp
    #temp1["bso_classification"] = temp1.apply(lambda row: fn.to_bso_class_with_ml(row["cleaned_feature"],logmodel),axis=1)
    context.log_event(
        AssetObservation(asset_key="temp1", metadata={
            "text_metadata": 'Vérification intermédiaire',
            "result": temp1["cleaned_feature"].head(2).to_json()})
    )
    temp1["bso_classification"] = logmodel.predict(temp1)
    temp.loc[temp[col].isin(temp1[col]), cols_to_replace] = temp1.loc[temp1[col].isin(temp[col]),cols_to_replace].values
    context.log_event(
        AssetObservation(asset_key="subset_of_all_classification_operations", metadata={
            "text_metadata": 'Subset résultat de toutes les opérations de classification',
            "size": f'nb lignes {temp.shape[0]}, nb cols {temp.shape[1]}'})
    )
    temp.to_csv(f'{context.op_config["model_output_data_path"]}/{context.op_config["observation_date"]}/uca_doi_classified.csv',index = False,encoding='utf8')
    return temp

@asset(config_schema={"primary_data_path": str})
def complete_classification_labels(context,temp):
    df_classification_mapping = pd.read_json(f'{context.op_config["primary_data_path"]}/bso_classification_mapping.json')
    temp_mapped=pd.merge(temp,df_classification_mapping, left_on='bso_classification', right_on='name_en',how="left").drop(columns=['name_en']).rename(columns={"name_fr": "bso_classification_fr"})
    return temp_mapped

@asset(config_schema={"model_output_data_path": str, "observation_date": str})
def complete_oa_with_classification(context,temp_mapped,publis_uniques_doi_oa_data):
    col = 'doi'
    cols_to_replace = ['bso_classification_fr','main_domain']
    #merge classification with main publis_uniques_doi_oa_data
    for c in cols_to_replace:
        publis_uniques_doi_oa_data[c] = np.nan
        publis_uniques_doi_oa_data.loc[publis_uniques_doi_oa_data[col].isin(temp_mapped[col]), cols_to_replace] = temp_mapped.loc[temp_mapped[col].isin(publis_uniques_doi_oa_data[col]),cols_to_replace].values
        #publications non classifiées en "unknown" + nettoyage \n dans labels du mesri
        publis_uniques_doi_oa_data[c] = publis_uniques_doi_oa_data[c].fillna("unknown").replace(r'\r\n', '', regex=True)
    context.log_event(
        AssetObservation(asset_key="publis_uniques_doi_oa_data", metadata={
            "text_metadata": 'Vérification intermédiaire',
            "result": publis_uniques_doi_oa_data[['bso_classification_fr','main_domain']].head(2).to_json()})
    )
    publis_uniques_doi_oa_data.to_csv(f'{context.op_config["model_output_data_path"]}/{context.op_config["observation_date"]}/publis_uniques_doi_oa_data_with_bsoclasses.csv', index= False,encoding='utf8')
    return publis_uniques_doi_oa_data

@job
def ml_classification():
    # datasets  
    publis_uniques_doi_oa_data = get_publis_uniques_doi_oa_data()
    mesri_bso_dataset = get_mesri_bso_dataset()
    # models
    logmodel = get_logmodel()
    # assets
    monitoring_publis_already_classified(mesri_bso_dataset,publis_uniques_doi_oa_data)
    classification = process_classification(mesri_bso_dataset,publis_uniques_doi_oa_data,logmodel)
    classification_labels = complete_classification_labels(classification)
    complete_oa_with_classification(classification_labels,publis_uniques_doi_oa_data)


@repository
def prod_bso_publis_scopus():
    return [ml_classification]

"""
Config
ops:
  complete_classification_labels:
    config:
      primary_data_path: bso_publis_scopus/03_primary
  complete_oa_with_classification:
    config:
      model_output_data_path: bso_publis_scopus/07_model_output
      observation_date: 2022-08-29
  get_bso_classes:
    config:
      bso_classes:
        '0': Biology (fond.)
        '1': Chemistry
        '2': Computer and information sciences
        '3': Earth, Ecology, Energy and applied biology
        '4': Engineering
        '5': Humanities
        '6': Mathematics
        '7': Medical research
        '8': Physical sciences, Astronomy
        '9': Social sciences
  get_logmodel:
    config:
      models_path: bso_publis_scopus/06_models
  get_publis_uniques_doi_oa_data:
    config:
      observation_date: 2022-08-29
      primary_data_path: bso_publis_scopus/03_primary
  process_classification:
    config:
      model_output_data_path: bso_publis_scopus/07_model_output
      observation_date: 2022-08-29
"""


