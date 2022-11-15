import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import requests
import json
import glob
import os
import helpers.unpaywall_harvest as upw
import helpers.crossref_harvest as crf
import helpers.dissemin_harvest as dsm
import helpers.functions as fn
import texthero as hero
import joblib
import pickle
import sqlite3

########################## 04_ML_MULTICLASSIFICATION ################################
bso_classes = {'0': 'Biology (fond.)', '1': 'Chemistry', '2': 'Computer and information sciences',
                '3': 'Earth, Ecology, Energy and applied biology', '4': 'Engineering', '5': 'Humanities',
                '6': 'Mathematics', '7': 'Medical research', '8': 'Physical sciences, Astronomy', '9': 'Social sciences', '<NA>': 'unknown'}
bso_classes_inverse = {'Biology (fond.)': '0', 'Chemistry': '1', 'Computer and \n information sciences': '2', 'Earth, Ecology, \nEnergy and applied biology': '3', 'Engineering': '4',
              'Humanities': '5', 'Mathematics': '6', 'Medical research': '7', 'Physical sciences, Astronomy': '8', 'Social sciences': '9'}

@asset
def get_mesri_bso_dataset():
    mesri_bso_dataset = pd.read_csv('https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.csv.gz',compression='gzip',encoding='utf8').convert_dtypes()
    return mesri_bso_dataset

@asset(required_resource_keys={"config_params"})
def get_publis_uniques_doi_oa_data(context):
    publis_uniques_doi_oa_data = pd.read_csv(f'{context.resources.config_params["primary_data_path"]}/{context.resources.config_params["observation_date"]}/publis_uniques_doi_oa_data.csv', sep=",", encoding="utf-8")
    return publis_uniques_doi_oa_data

@op(required_resource_keys={"config_params"})
def get_logmodel(context):
    logmodel = joblib.load(f'{context.resources.config_params["models_path"]}/logmodel.joblib')
    return logmodel
	
@op(required_resource_keys={"config_params"})
def get_bso_classes(context):
    bso_classes = dict(context.resources.config_params["bso_classes"])
    return bso_classes

@op(required_resource_keys={"config_params"})
def monitoring_publis_already_classified(context,df_bso,publis_uniques_doi_oa_data):
    list_uca_doi = publis_uniques_doi_oa_data["doi"].tolist()
    context.log_event(
        AssetObservation(asset_key="publis_already_classified", metadata={
            "text_metadata": 'Pourcentage des publis UCA présentes dans le dataset du ministère et pour lesquelles on peut déjà récupérer la bso_classification',
            "size": "{}".format(round(len(df_bso[df_bso['doi'].isin(list_uca_doi)]['doi'].tolist()) / len(list_uca_doi) * 100))})
    )
    classified = df_bso[(df_bso['doi'].isin(list_uca_doi)) & (df_bso["bso_classification"] != "unknown")][['doi','bso_classification']]
    classified["bso_classification_classe"] = classified["bso_classification"].apply(lambda x: bso_classes_inverse[x.strip()])
    classified["source"] = "mesri"
    classified = classified.drop(columns=['bso_classification'])
    classified.to_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/publis_already_classified.csv', index=False, encoding="utf-8")
    return classified

@op(required_resource_keys={"config_params"})
def monitoring_publis_not_classified(context,publis_uniques_doi_oa_data, publis_already_classified):
    list_classified_doi = publis_already_classified["doi"].tolist()
    not_classified = publis_uniques_doi_oa_data[~publis_uniques_doi_oa_data['doi'].isin(list_classified_doi)][['doi','title','journal_name','publisher_by_doiprefix']]
    not_classified["feature"] = not_classified["title"] + ' '  +  not_classified["journal_name"] + ' '  +  not_classified["publisher_by_doiprefix"]
    not_classified['cleaned_feature'] = (
            not_classified['feature']
            .pipe(hero.clean)
            .apply(fn.remove_stopwords_fr)
            .apply(fn.remove_special_characters)
            .apply(fn.lemmatize)
    )
    not_classified = not_classified.dropna().drop(columns=['title','journal_name','publisher_by_doiprefix','feature'])
    context.log_event(
        AssetObservation(asset_key="publis_not_classified", metadata={
            "text_metadata": 'Shape des données sur lesquelles va tourner le modèle ML',
            "size": f'{not_classified.shape}',
            "result": not_classified[["doi","cleaned_feature"]].head(2).to_json()})
    )
    not_classified.to_csv(f'{context.resources.config_params["model_input_data_path"]}/publis_not_classified.csv', index=False, encoding="utf-8")
    return not_classified

@op(required_resource_keys={"config_params"})
def process_classification(context,publis_not_classified,logmodel):
    publis_new_classified = publis_not_classified
    #le pipeline du model possède uen classe ColumnTransformer qui gère automatiquement le dataframe avec la colonne cleand_feature
    publis_new_classified["bso_classification_classe"] = logmodel.predict(publis_new_classified).astype(str)
    publis_new_classified["source"] = "local"
    publis_new_classified = publis_new_classified.drop(columns=['cleaned_feature'])
    publis_new_classified.to_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/publis_new_classified.csv', index=False, encoding="utf-8")
    return publis_new_classified

@op(required_resource_keys={"config_params"})
def monitoring_publis_all_classified(context,publis_already_classified, publis_new_classified):
  publis_all_classified = pd.concat([publis_already_classified, publis_new_classified]).rename(columns={"doi": "tmp_doi"})
  publis_all_classified['bso_classification_classe'] = publis_all_classified['bso_classification_classe'].astype("string")
  context.log_event(
        AssetObservation(asset_key="publis_all_classified", metadata={
            "text_metadata": 'Shape du résultata de la classification',
            "size": f'{publis_all_classified.shape}',
            "result": publis_all_classified.head(2).to_json()})
  )
  publis_all_classified.to_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/publis_all_classified.csv', index=False, encoding="utf-8")
  return publis_all_classified


@asset(required_resource_keys={"config_params"})
def complete_oa_with_classification(context,publis_uniques_doi_oa_data, publis_all_classified):
  publis_uniques_doi_oa_data_with_bsoclasses = pd.merge(publis_uniques_doi_oa_data, publis_all_classified, left_on="doi", right_on="tmp_doi", how="left").drop(columns=['tmp_doi'])
  #publis_uniques_doi_oa_data_with_bsoclasses[publis_uniques_doi_oa_data_with_bsoclasses.bso_classification_classe.notna()]["bso_classification"] = publis_uniques_doi_oa_data_with_bsoclasses[publis_uniques_doi_oa_data_with_bsoclasses.bso_classification_classe.notna()]["bso_classification_classe"].apply(lambda x: bso_classes[str(x)])
  publis_uniques_doi_oa_data_with_bsoclasses["bso_classification"] = publis_uniques_doi_oa_data_with_bsoclasses["bso_classification_classe"].apply(lambda x: bso_classes[str(x)])
  publis_uniques_doi_oa_data_with_bsoclasses['source'] = publis_uniques_doi_oa_data_with_bsoclasses['source'].replace(np.nan, 'local')
  publis_uniques_doi_oa_data_with_bsoclasses.to_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/publis_uniques_doi_oa_data_with_bsoclasses.csv', index= False,encoding='utf8')
  return publis_uniques_doi_oa_data_with_bsoclasses

@job(name="00_test_ml",
     resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("1. ")
    }
)
def dev_ml():
  # datasets  
  publis_uniques_doi_oa_data = get_publis_uniques_doi_oa_data()
  mesri_bso_dataset = get_mesri_bso_dataset()
  # models
  logmodel = get_logmodel()
  # assets
  publis_already_classified = monitoring_publis_already_classified(mesri_bso_dataset,publis_uniques_doi_oa_data)
  publis_not_classified = monitoring_publis_not_classified(publis_uniques_doi_oa_data, publis_already_classified)
  publis_new_classified = process_classification(publis_not_classified,logmodel)
  publis_all_classified = monitoring_publis_all_classified(publis_already_classified, publis_new_classified)
  complete_oa_with_classification(publis_uniques_doi_oa_data, publis_all_classified)


@repository
def dev_bso_publis_scopus():
    return [dev_ml]

"""
resources:
  config_params:
    config:
      db_path: bso_publis_scopus/09_db/publications.db
      raw_data_path: bso_publis_scopus/01_raw
      intermediate_data_path: bso_publis_scopus/02_intermediate
      primary_data_path: bso_publis_scopus/03_primary
      feature_data_path: bso_publis_scopus/04_feature
      model_input_data_path: bso_publis_scopus/05_model_input
      models_path: bso_publis_scopus/06_models
      model_output_data_path: bso_publis_scopus/07_model_output
      reporting_data_path: bso_publis_scopus/08_reporting
      observation_date: 2022-08-29
      bso_classes: {'0': 'Biology (fond.)', '1': 'Chemistry', '2': 'Computer and information sciences',
                    '3': 'Earth, Ecology, Energy and applied biology', '4': 'Engineering', '5': 'Humanities',
                    '6': 'Mathematics', '7': 'Medical research', '8': 'Physical sciences, Astronomy', '9': 'Social sciences'}
      corpus_end_year: 2022
"""


