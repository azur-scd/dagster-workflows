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

########################## 01_CREATE_SUBFOLDERS ################################

@op(required_resource_keys={"config_params"})
def create_temp_subfolders(context):
    enrichments_list = ["upw","crf","dsm"]
    for i in enrichments_list:
        if not os.path.exists(f'{context.resources.config_params["intermediate_data_path"]}/temp_{i}'):
            os.mkdir(f'{context.resources.config_params["intermediate_data_path"]}/temp_{i}')  

@op(required_resource_keys={"config_params"})
def create_data_observation_subfolders(context):
    concerned_folders = ["primary_data_path","model_output_data_path","reporting_data_path"]
    for i in concerned_folders:
        if not os.path.exists(f'{context.resources.config_params[i]}/{context.resources.config_params["observation_date"]}'):
            os.mkdir(f'{context.resources.config_params[i]}/{context.resources.config_params["observation_date"]}')
            if i == "primary_data_path":
                os.mkdir(f'{context.resources.config_params["primary_data_path"]}/{context.resources.config_params["observation_date"]}/publis_non_traitees')
            if i == "reporting_data_path":
                os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse')
                os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation')
                os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/docelec')
                os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access')
                os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access/consolidation')

########################## 02_MAIN_TRANSFORM_PROCESS ################################

@asset(required_resource_keys={"config_params"})
def extract_data_source(context):
    df = pd.read_json(f'{context.resources.config_params["raw_data_path"]}/{context.resources.config_params["observation_date"]}/exportDonnees_barometre_complet_{context.resources.config_params["observation_date"]}.json')
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
    # archive publis sans doi non traitées
    df_reference_data_sans_doi = df_reference_data[df_reference_data["doi"].isna()]
    df_reference_data_sans_doi.to_csv(f'{context.resources.config_params["primary_data_path"]}/{context.resources.config_params["observation_date"]}/publis_non_traitees/source_data_scopus_sans_doi.csv',index = False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="df_reference_data_sans_doi", metadata={
            "text_metadata": 'Shape of the number of publis without doi from initial JP dataset',
            "size": f"{df_reference_data_sans_doi.shape[0]/df_reference_data.shape[0]*100}%"})
    )
    return df_reference_data

@asset(required_resource_keys={"config_params"})
def update_referentiel_data(context,df_reference_data):
    # fetch live data
    df_affiliations = pd.read_json(f'{context.resources.config_params["primary_data_path"]}/referentiel_structures.json')
    # rename and save the file in his last state
    df_affiliations.to_json(f'{context.resources.config_params["primary_data_path"]}/referentiel_structures_old.json',orient="records",indent=3,force_ascii=False)
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
    df_affiliations.to_json(f'{context.resources.config_params["primary_data_path"]}/referentiel_structures.json',orient="records",indent=3,force_ascii=False)
    context.log_event(
        AssetObservation(asset_key="new_affiliations_dataset", metadata={
            "text_metadata": 'Shape of the new affiliations dataset',
            "size": f"nb lignes {df_affiliations.shape[0]}, nb cols {df_affiliations.shape[1]}"})
        )
    return df_affiliations

@asset(required_resource_keys={"config_params"})
def get_referentiel_structures(context):
    df = pd.read_json(f'{context.resources.config_params["primary_data_path"]}/referentiel_structures.json')
    return df

@asset(required_resource_keys={"config_params"})
def transform_publis_all_with_affiliations_data(context,df_reference_data,df_affiliations):
    # merge all publis with affiliations
    df_affiliations["affiliation_id"] = df_affiliations["affiliation_id"].astype('str')
    df_reference_data["aff_scopus_id"] = df_reference_data["aff_scopus_id"].astype('str')
    publis_all_with_affiliations_data = pd.merge(df_reference_data,df_affiliations[df_affiliations["affiliation_id"].notna()], left_on='aff_scopus_id', right_on='affiliation_id',how="left").drop(columns=['affiliation_id','documents_count','ppn_valide','affcourt_valide','RNSR','VIAF','ISNI','BNF','HAL'])
    publis_all_with_affiliations_data = publis_all_with_affiliations_data.rename(columns={'id': 'aff_internal_id', 'parent_id': 'aff_parent_id'})
    # identify corresponding author if UCA
    publis_all_with_affiliations_data["corresponding"] = publis_all_with_affiliations_data[publis_all_with_affiliations_data["corresponding_author"] == "oui"].apply (lambda row: fn.keep_duplicate(row), axis=1)
    publis_all_with_affiliations_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access/publis_all_with_affiliations_data.csv',index = False,encoding='utf8')
    return publis_all_with_affiliations_data

@asset(required_resource_keys={"config_params"})
def get_publis_all_with_affiliations_data(context):    
    publis_all_with_affiliations_data = pd.read_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access/publis_all_with_affiliations_data.csv',sep=",",encoding='utf8')
    return publis_all_with_affiliations_data

@asset(required_resource_keys={"config_params"})
def create_publis_uniques_doi_data(context,publis_all_with_affiliations_data):
    # Deduplicate
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].astype('category')
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].cat.set_categories(['oui', 'non', 'corr absent pour cette publi'], ordered=True)
    publis_all_with_affiliations_data.sort_values(by=['doi', 'corresponding_author'])
    publis_uniques_doi_data = publis_all_with_affiliations_data[publis_all_with_affiliations_data.doi.notna()].drop_duplicates(subset=['doi'], keep='first')[["source_id","doi","year","corresponding","all_authors"]]
    publis_uniques_doi_data = publis_uniques_doi_data[publis_uniques_doi_data.year < context.resources.config_params["corpus_end_year"]]
    publis_uniques_doi_data.to_csv(f'{context.resources.config_params["intermediate_data_path"]}/publis_uniques_doi_data.csv',index = False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="unique_uca_doi", metadata={
            "text_metadata": 'Number of unique publis with doi',
            "size": f'nb lignes {publis_uniques_doi_data["doi"].isna().shape[0]}, nb cols {publis_uniques_doi_data.shape[1]}'})
        )
    return publis_uniques_doi_data

########################## 03_UNPAYWALL_AND_PUBLISHERS_PREFIX ################################

@asset(required_resource_keys={"config_params"})
def get_publis_uniques_doi_data(context):
    publis_uniques_doi_data = pd.read_csv(f'{context.resources.config_params["intermediate_data_path"]}/publis_uniques_doi_data.csv',sep=",",encoding='utf8')
    return publis_uniques_doi_data

@asset(required_resource_keys={"config_params"})
def get_publishers_doi_prefix(context):
    # LA MAJ PREALABLE AVEC LES NOUVEAUX PREFIXES SE FAIT DANS LE NOTEBOOK DE CONTROLE
    publishers_doi_prefix = pd.read_csv(f'{context.resources.config_params["primary_data_path"]}/mapping_doiprefixes_publisher.csv', sep=",",encoding='utf8').drop_duplicates(subset=['prefix'], keep='last')
    context.log_event(
        AssetObservation(asset_key="new_publisher_doiprefix", metadata={
            "text_metadata": 'Number of new publishers doi prefix',
            "size": f'nb lignes {publishers_doi_prefix.shape[0]}'})
        )
    publishers_doi_prefix.to_csv(f'{context.resources.config_params["primary_data_path"]}/mapping_doiprefixes_publisher.csv', index = False,encoding='utf8')
    return publishers_doi_prefix

@asset(required_resource_keys={"config_params"})
def get_unpaywall_data(context,publis_uniques_doi_data):
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        #print("DOI traités par unpaywall : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        upw.upw_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv(f'{context.resources.config_params["intermediate_data_path"]}/temp_upw/upw_'+str(i)+'.csv',index = False,encoding='utf8')
    #concaténation des cvs
    all_filenames = [i for i in glob.glob(f'{context.resources.config_params["intermediate_data_path"]}/temp_upw/*.csv')]
    unpaywall_data = pd.concat([pd.read_csv(f).drop(columns=['oa_locations']) for f in all_filenames ]).drop_duplicates(subset=['source_doi'], keep='last')
    unpaywall_data.to_csv(f'{context.resources.config_params["intermediate_data_path"]}/unpaywall_data.csv',index = False,encoding='utf8')
    return unpaywall_data

@asset(required_resource_keys={"config_params"})
def merge_all_data(context,publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data):
    # unpaywall data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_data,unpaywall_data, left_on='doi', right_on='source_doi',how="right").drop(columns=['source_doi','year_upw'])
    # publishers doi prefix
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data.apply (lambda row: str(row["doi"].partition("/")[0]), axis=1) 
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data["doi_prefix"].astype(str)
    publishers_doi_prefix["prefix"] = publishers_doi_prefix["prefix"].astype(str)
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(publishers_doi_prefix, left_on='doi_prefix', right_on='prefix',how='left').drop(columns=['prefix'])
    publis_uniques_doi_oa_data.to_csv(f'{context.resources.config_params["primary_data_path"]}/{context.resources.config_params["observation_date"]}/publis_uniques_doi_oa_data.csv', index= False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="unique_uca_doi_with_oa", metadata={
            "text_metadata": 'Number of unique publis with doi and oa metadata',
            "size": f'nb lignes {publis_uniques_doi_oa_data.shape[0]}, nb cols {publis_uniques_doi_oa_data.shape[1]}'})
        )
    return publis_uniques_doi_oa_data

########################## 04_ML_MULTICLASSIFICATION ################################

bso_classes = {'0': 'Biology (fond.)', '1': 'Chemistry', '2': 'Computer and information sciences',
                '3': 'Earth, Ecology, Energy and applied biology', '4': 'Engineering', '5': 'Humanities',
                '6': 'Mathematics', '7': 'Medical research', '8': 'Physical sciences, Astronomy', '9': 'Social sciences', '<NA>': 'unknown'}
bso_classes_inverse = {'Biology (fond.)': '0', 'Chemistry': '1', 'Computer and \n information sciences': '2', 'Earth, Ecology, \nEnergy and applied biology': '3', 'Engineering': '4',
              'Humanities': '5', 'Mathematics': '6', 'Medical research': '7', 'Physical sciences, Astronomy': '8', 'Social sciences': '9'}
bso_classes_trad = {"0": "Biologie fondamentale", 
                    "1": "Chimie", 
                    "2": "Informatique et sciences de l'information", 
                    "3": "Sciences de la Terre, écologie, énergie et biologie appliquée", 
                    "4": "Ingénierie",
                    "5": "Sciences humaines", 
                    "6": "Mathématiques", 
                    "7": "Recherche médicale", 
                    "8": "Sciences physiques et astronomie", 
                    "9": "Sciences sociales",
                    "<NA>": "Inconnu"}
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
	
"""@op(required_resource_keys={"config_params"})
def get_bso_classes(context):
    bso_classes = dict(context.resources.config_params["bso_classes"])
    return bso_classes"""

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
  publis_uniques_doi_oa_data_with_bsoclasses["bso_classification_en"] = publis_uniques_doi_oa_data_with_bsoclasses["bso_classification_classe"].apply(lambda x: bso_classes[str(x)])
  publis_uniques_doi_oa_data_with_bsoclasses["bso_classification_fr"] = publis_uniques_doi_oa_data_with_bsoclasses["bso_classification_classe"].apply(lambda x: bso_classes_trad[str(x)])
  publis_uniques_doi_oa_data_with_bsoclasses['source'] = publis_uniques_doi_oa_data_with_bsoclasses['source'].replace(np.nan, 'local')
  publis_uniques_doi_oa_data_with_bsoclasses.to_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/publis_uniques_doi_oa_data_with_bsoclasses.csv', index= False,encoding='utf8')
  return publis_uniques_doi_oa_data_with_bsoclasses

########################## 05_CROSSREF_AND_DISSEMIN ################################

@asset(required_resource_keys={"config_params"})
def get_publis_uniques_doi_oa_data_with_bsoclasses(context):
    publis_uniques_doi_oa_data_with_bsoclasses = pd.read_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/publis_uniques_doi_oa_data_with_bsoclasses.csv', sep=",", encoding="utf-8")
    return publis_uniques_doi_oa_data_with_bsoclasses

@asset(required_resource_keys={"config_params"})
def get_crossref_data(context,publis_uniques_doi_oa_data_with_bsoclasses):
    l = publis_uniques_doi_oa_data_with_bsoclasses["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        #print("DOI traités par crossref : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        crf.crf_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv(f'{context.resources.config_params["intermediate_data_path"]}/temp_crf/crf_'+str(i)+".csv",index = False,encoding='utf8')
    # concaténation des csv
    all_filenames = [i for i in glob.glob(f'{context.resources.config_params["intermediate_data_path"]}/temp_crf/*.csv')]
    combined_crf = pd.concat([pd.read_csv(f) for f in all_filenames ])
    crossref_data = combined_crf[combined_crf.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    crossref_data.to_csv(f'{context.resources.config_params["intermediate_data_path"]}/crossref_data.csv',index = False,encoding='utf8')
    return crossref_data

@asset(required_resource_keys={"config_params"})
def get_dissemin_data(context,publis_uniques_doi_oa_data_with_bsoclasses):
    l = publis_uniques_doi_oa_data_with_bsoclasses["doi"].to_list()
    n = 10
    for i in range(0, len(l), n):
        #print("DOI traités par dissemin : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        dsm.dsm_retrieval(l[i:i+n]).to_csv(f'{context.resources.config_params["intermediate_data_path"]}/temp_dsm/dsm_'+str(i)+'.csv',index = False,encoding='utf8')
    #concaténation des cvs
    all_filenames = [i for i in glob.glob(f'{context.resources.config_params["intermediate_data_path"]}/temp_dsm/*.csv')]
    combined_dsm = pd.concat([pd.read_csv(f) for f in all_filenames ])
    dissemin_data = combined_dsm[combined_dsm.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    dissemin_data.to_csv(f'{context.resources.config_params["intermediate_data_path"]}/dissemin_data.csv',index = False,encoding='utf8')
    return dissemin_data

@asset(required_resource_keys={"config_params"})
def temp_get_crossref_data(context):
    crossref_data = pd.read_csv(f'{context.resources.config_params["intermediate_data_path"]}/crossref_data.csv',sep=",",encoding='utf8')
    return crossref_data

@asset(required_resource_keys={"config_params"})
def temp_get_dissemin_data(context):
    dissemin_data = pd.read_csv(f'{context.resources.config_params["intermediate_data_path"]}/dissemin_data.csv',sep=",",encoding='utf8')
    return dissemin_data

@asset(required_resource_keys={"config_params"})
def merge_data(context,publis_uniques_doi_oa_data_with_bsoclasses,crossref_data, dissemin_data):
    # crossref data
    publis_uniques_doi_oa_data_with_bsoclasses_complete = publis_uniques_doi_oa_data_with_bsoclasses.merge(crossref_data, left_on='doi', right_on='source_doi',how='left').drop(columns=['source_doi','published-online-date','journal-published-print-date','published-print-date'])
    # dissemin data
    publis_uniques_doi_oa_data_with_bsoclasses_complete = pd.merge(publis_uniques_doi_oa_data_with_bsoclasses_complete,dissemin_data, left_on='doi', right_on='source_doi',how="left").drop(columns=['source_doi'])
    publis_uniques_doi_oa_data_with_bsoclasses_complete.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access/publis_uniques_doi_oa_data_with_bsoclasses_complete.csv', index= False,encoding='utf8')
    context.log_event(
        AssetObservation(asset_key="shape_final_dataset", metadata={
            "text_metadata": 'Number of unique publis with doi and oa metadata',
            "size": f'nb lignes {publis_uniques_doi_oa_data_with_bsoclasses_complete.shape[0]}, nb cols {publis_uniques_doi_oa_data_with_bsoclasses_complete.shape[1]}'})
        )
    return publis_uniques_doi_oa_data_with_bsoclasses_complete
	
@asset(required_resource_keys={"config_params"})
def get_publis_uniques_doi_oa_data_with_bsoclasses_complete(context):
    publis_uniques_doi_oa_data_with_bsoclasses_complete = pd.read_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access/publis_uniques_doi_oa_data_with_bsoclasses_complete.csv', sep=",", encoding="utf-8")
    return publis_uniques_doi_oa_data_with_bsoclasses_complete

########################## 06_SAVE_SQLITE ################################

@op(required_resource_keys={"config_params"})
def db_connexion(context):
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    return conn

@op(required_resource_keys={"config_params"})
def create_bso_publis_uniques_table(context,df):
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bso_publis_uniques_{observation_date}")
    cur.execute(f"CREATE TABLE bso_publis_uniques_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"bso_publis_uniques_{observation_date}", conn, if_exists='append', index=False)

@op(required_resource_keys={"config_params"})
def create_bso_publis_all_by_affiliation_table(context,df):
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bso_publis_all_by_affiliation_{observation_date}")
    cur.execute(f"CREATE TABLE bso_publis_all_by_affiliation_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"bso_publis_all_by_affiliation_{observation_date}", conn, if_exists='append', index=False)

@op(required_resource_keys={"config_params"})
def create_referentiel_structures_table(context,df):
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS referentiel_structures_{observation_date}")
    cur.execute(f"CREATE TABLE referentiel_structures_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"referentiel_structures_{observation_date}", conn, if_exists='append', index=False)

########################## JOBS ################################
################################################################

@job(name="01_create_subfolders",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("1. Création de l'architecture de dossiers nécessaire ; 2. NE PAS OUBLIER D'EXECUTER LE NOTEBOOK DE CONTROLE AVANT LES JOBS")
    }
)
def create_subfolders():
    create_temp_subfolders()
    create_data_observation_subfolders()


@job(name="02_main_transform_process",
    resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("Attention : le fichier 08_reporting/DATE/publis_all_with_affiliations_data.csv contient toutes les publis avec ou sans DOI + les publis les plus récentes jusqu'à la date de moissonnage de Scopus")
    }
)
def main_transform_process():
    data_source = extract_data_source()
    referentiel_data = update_referentiel_data(data_source)
    publis_all_with_affiliations_data = transform_publis_all_with_affiliations_data(data_source,referentiel_data)
    #control_missing_doi(publis_all_with_affiliations_data)
    create_publis_uniques_doi_data(publis_all_with_affiliations_data)

@job(name="03_unpaywall_and_publishers_doiprefix_process",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("Ajout des données Unpaywall et des données Crossref sur les noms d'éditeurs à partir des préfixe de DOi des publis")
    }
)
def unpaywall_and_publishers_doiprefix_process():
    publishers_doi_prefix = get_publishers_doi_prefix()
    publis_uniques_doi_data = get_publis_uniques_doi_data()
    unpaywall_data = get_unpaywall_data(publis_uniques_doi_data)
    merge_all_data(publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data)

@job(name="04_ml_multiclassification_process",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("")
    }
)
def ml_multiclassification_process():
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

@job(name="05_crossref_and_dissemin_data_process",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("")
    }
)
def crossref_and_dissemin_data_process():
    #configs
    publis_uniques_doi_oa_data_with_bsoclasses = get_publis_uniques_doi_oa_data_with_bsoclasses()
    crossref_data = get_crossref_data(publis_uniques_doi_oa_data_with_bsoclasses)
    dissemin_data = get_dissemin_data(publis_uniques_doi_oa_data_with_bsoclasses)
    #crossref_data = temp_get_crossref_data()
    #dissemin_data = temp_get_dissemin_data()
    merge_data(publis_uniques_doi_oa_data_with_bsoclasses,crossref_data, dissemin_data)

@job(name="06_sqlite_save_process",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("")
    })
def sqlite_save_process():
    publis_uniques_doi_oa_data_with_bsoclasses = get_publis_uniques_doi_oa_data_with_bsoclasses()
    publis_all_with_affiliations_data = get_publis_all_with_affiliations_data()
    referentiel_structures = get_referentiel_structures()
    create_bso_publis_uniques_table(publis_uniques_doi_oa_data_with_bsoclasses)
    create_bso_publis_all_by_affiliation_table(publis_all_with_affiliations_data)
    create_referentiel_structures_table(referentiel_structures)


@repository
def prod_bso_publis_scopus():
    return [create_subfolders,main_transform_process,unpaywall_and_publishers_doiprefix_process,ml_multiclassification_process,crossref_and_dissemin_data_process,sqlite_save_process]

"""
Config
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
	  bso_classes: {0: 'Biology (fond.)', 1: 'Chemistry', 2: 'Computer and \n information sciences',
                    3: 'Earth, Ecology, \nEnergy and applied biology', 4: 'Engineering', 5: 'Humanities',
                    6: 'Mathematics', 7: 'Medical research', 8: 'Physical sciences, Astronomy', 9: 'Social sciences'}
      bso_classes_inverse: {'Biology (fond.)': 0, 'Chemistry': 1, 'Computer and \n information sciences': 2, 'Earth, Ecology, \nEnergy and applied biology': 3, 'Engineering': 4,
              'Humanities': 5, 'Mathematics': 6, 'Medical research': 7, 'Physical sciences, Astronomy': 8, 'Social sciences': 9}
      corpus_end_year: 2022
  
"""


