import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
from sentence_transformers import SentenceTransformer, util
from nltk import word_tokenize 
from nltk.util import ngrams
import pandas as pd
import numpy as np
import glob
import helpers.functions as fn
import sqlite3

########################## [OLD] 01_MAIN_FUZZY ################################

def apply_model(model,univ_forme,affil_forme):
    model = model
    cos_sin_list = list()
    for i in affil_forme:
        # util.cos_sim(emb2, model.encode(str(i))) est un objet de type pytorch.tensor
        cos_sin_list.append(util.cos_sim(model.encode(univ_forme), model.encode((str(i)))).item())
    return cos_sin_list

def flatten(l):
    return [item for sublist in l for item in sublist]

def get_ngrams(l,num):
    l = [x.replace("'","") for x in l]
    line_flat = " ".join(l)
    n_grams = ngrams(word_tokenize(line_flat), num)
    return [' '.join(grams) for grams in n_grams]

@op(required_resource_keys={"config_params"})
def load_model(context):
    model = SentenceTransformer(f'{context.resources.config_params["bert_model"]}')
    return model

@asset(required_resource_keys={"config_params"})
def extract_data_source(context):
    df = pd.read_json(f'{context.resources.config_params["raw_data_path"]}/{context.resources.config_params["observation_date"]}/exportDonnees_barometre_complet_{context.resources.config_params["observation_date"]}.json')
    # keep columns
    df_sample = df[["dc:identifiers","prism:doi","reference","annee_pub","@afids","mentionAffil_reconstruct","@auid","ce:indexed-name", '@orcid',"corresponding_author","Is_dc:creator"]].sample(50)
    context.log_event(
        AssetObservation(asset_key="initial_jp_dataset", metadata={
            "text_metadata": 'Shape of the sample JP dataset',
            "size": f"nb lignes {df_sample.shape[0]}, nb cols {df_sample.shape[1]}"})
    )
    return df_sample


@op
def transform_nlp_data(df):
    df['mentionAffil_reconstruct_subsentence_cleaned'] = df['mentionAffil_reconstruct'].apply(fn.str2list).apply(fn.list2str)
    df['mentionAffil_onegrams'] = df['mentionAffil_reconstruct_subsentence_cleaned'].apply(lambda x: get_ngrams(x,1))
    df['mentionAffil_trigrams'] = df['mentionAffil_reconstruct_subsentence_cleaned'].apply(lambda x: get_ngrams(x,3))
    return df

@op
def get_bert_uca_dvp(df,model):
    data = df[["dc:identifiers","mentionAffil_trigrams"]].rename(columns={"dc:identifiers": "id", "mentionAffil_trigrams": "mention"})
    data["score_bert_uca_dvp"] = data["mention"].apply(lambda x : apply_model(model,"universite cote azur",x))
    data["max_score_bert_uca_dvp"] = data["score_bert_uca_dvp"].apply(lambda x: round(max(x),1) if  len(x)  != 0 else 0)
    data["position_max_score_bert_uca_dvp"] = data["score_bert_uca_dvp"].apply(lambda x: x.index(max(x)) if len(x) != 0 else None)
    return data

@op
def get_bert_uns_dvp(df,model):
    data = df[["dc:identifiers","mentionAffil_trigrams"]].rename(columns={"dc:identifiers": "id", "mentionAffil_trigrams": "mention"})
    data["score_bert_uns_dvp"] = data["mention"].apply(lambda x : apply_model(model,"universite nice sophia antipolis",x))
    data["max_score_bert_uns_dvp"] = data["score_bert_uns_dvp"].apply(lambda x:round(max(x),1) if len(x) != 0 else 0)
    return data

@op
def get_fuzzy_sigles(df):
    data = df[["dc:identifiers","mentionAffil_onegrams"]].rename(columns={"dc:identifiers": "id", "mentionAffil_onegrams": "mention"})
    data["score_fuzzy_uca_sigle"] = data["mention"].apply(fn.fuzzy_uca_sigle)
    data["score_fuzzy_uns_sigle"] = data["mention"].apply(fn.fuzzy_uns_sigle)
    return data

@op
def merge_nlp_process(df,data):
    return pd.merge(df,data,left_on="dc:identifiers", right_on="id", how="left").drop(columns=['id','mention'])

@asset(required_resource_keys={"config_params"})
def save(context,df):
  df.to_csv(f'{context.resources.config_params["dev_data_path"]}/df_uca_dvp_test.csv', index=False, encoding="utf-8")


@job(name="01_test_bert",
     resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("1. ")
    }
)
def dev_bert():
    #configs
    model = load_model()
    data_source = extract_data_source()
    nlp_data = transform_nlp_data(data_source)
    bert_uca_dvp = get_bert_uca_dvp(nlp_data,model)
    bert_uns_dvp = get_bert_uns_dvp(nlp_data,model)
    fuzzy_sigles = get_fuzzy_sigles(nlp_data)
    merged_nlp_process = merge_nlp_process(merge_nlp_process(merge_nlp_process(nlp_data,bert_uca_dvp),bert_uns_dvp), fuzzy_sigles)
    save(merged_nlp_process)

@repository
def dev_bso_publis_scopus():
    return [dev_bert]


"""
Config
resources:
  config_params:
    config:
      bert_model: all-mpnet-base-v2
      db_path: bso_publis_scopus/09_db/publications.db
      dev_data_path: bso_publis_scopus/00_dev
      raw_data_path: bso_publis_scopus/01_raw
      intermediate_data_path: bso_publis_scopus/02_intermediate
      primary_data_path: bso_publis_scopus/03_primary
      feature_data_path: bso_publis_scopus/04_feature
      model_input_data_path: bso_publis_scopus/05_model_input
      models_path: bso_publis_scopus/06_models
      model_output_data_path: bso_publis_scopus/07_model_output
      reporting_data_path: bso_publis_scopus/08_reporting
      observation_date: 2022-08-29
      corpus_end_year: 2022
"""


