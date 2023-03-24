import graphlib
from dagster import job, op, graph, repository, asset, multi_asset, AssetIn, AssetOut, AssetMaterialization, AssetObservation, Out, Output, In, String, MetadataValue, make_values_resource
from typing import Tuple
import pandas as pd
import numpy as np
import requests
import json
import glob
import os
import sqlite3
import helpers.scopus_harvest as scp
import helpers.unpaywall_harvest as upw
import helpers.doaj_harvest as doaj
import helpers.functions as fn

########################## 01_CREATE_SUBFOLDERS ################################ 

@op(required_resource_keys={"config_params"})
def create_subfolders(context):
    concerned_folders = ["01_raw","02_intermediate"]
    enrichments_list = ["upw"]
    for i in concerned_folders:
        if not os.path.exists(f'bso_publis/{i}/{context.resources.config_params["observation_date"]}'):
            os.mkdir(f'bso_publis/{i}/{context.resources.config_params["observation_date"]}')
            if i == "01_raw":
                os.mkdir(f'bso_publis/{i}/{context.resources.config_params["observation_date"]}/scopus')
                os.mkdir(f'bso_publis/{i}/{context.resources.config_params["observation_date"]}/hal')
            if i == "02_intermediate":
                for e in enrichments_list:
                    if not os.path.exists(f'bso_publis/{i}/{context.resources.config_params["observation_date"]}/temp_{e}'):
                        os.mkdir(f'bso_publis/{i}/{context.resources.config_params["observation_date"]}/temp_{e}')  

########################## 02_HARVEST_SCOPUS #######################################

@asset(required_resource_keys={"config_params"})
def get_referentiel_structures(context):
    url = f'{context.resources.config_params["url_middleware_publications_prod"]}/structures'
    response = requests.request("GET", url, verify=False).text
    data = json.loads(response)
    df = pd.DataFrame(data)
    df["aff_label"] = df[['label', 'acronyme']].apply(lambda x: ' - '.join(x.dropna()), axis=1)
    df.to_csv(f'bso_publis/01_raw/{context.resources.config_params["observation_date"]}/scopus/referentiel_structures.csv', index=False, encoding="utf-8")
    return df

# dataframe des structures reconnues dans Scopus (aff_scopus_id non temporaires) hors UCA et OCA labos inconnus
@asset
def get_df_affil_id(df):
    return df.loc[
        (df['harvest_start'].str.startswith('2', na=False))
        & (~df['aff_scopus_id'].str.startswith('temp_', na=False))
        & (~df["aff_scopus_id"].isin(["60110693", "60010513"]))
    ]

# dataframe des structures non encore reconnues dans Scopus avec aff_scopus_id temporaires
@asset
def get_df_affil_org(df):
    return df.loc[
        (df['harvest_start'].str.startswith('2', na=False))
        & (df['aff_scopus_id'].str.startswith('temp_', na=False))
    ]

# dataframe des structures génériques UCA et OCA sans labo
@asset
def get_df_affil_sanslabo(df):
    return df.loc[
        df['aff_scopus_id'].isin(["60110693", "60010513"])
    ]

# dataframe du triplet aff_internal_id/aff_scopus_id/dc_identifiers pour les structures reconnues dans Scopus
@asset
def get_affid_scopusid_pairs(df_affil_id):
    return pd.concat([
        scp.get_list_publis_ids_by_affid(
            row[0], row[1], start_year=row[2], end_year=row[3]
        )
        for row in zip(df_affil_id['id'],df_affil_id['aff_scopus_id'],df_affil_id['harvest_start'], df_affil_id['harvest_end'])
    ], axis=0)

# dataframe du triplet aff_internal_id/aff_scopus_id/dc_identifiers pour les structures non reconnues dans Scopus (temp_...)
@asset
def get_afforg_scopusid_pairs(df_affil_org):
    return pd.concat([
        scp.get_list_publis_ids_by_afforg(
            row[0], row[1], row[2], start_year=row[3], end_year=row[4]
        )
        for row in zip(df_affil_org['id'],df_affil_org['aff_scopus_id'],df_affil_org['acronyme'],df_affil_org['harvest_start'], df_affil_org['harvest_end'])
    ], axis=0)

# dataframe du triplet aff_internal_id/aff_scopus_id/dc_identifiers pour les structures UCA et OCA sans labo
@asset
def get_sanslabo_affid_scopusid_pairs(df_sans_labo, df_affil_id, df_affil_org):
    return pd.concat([
        scp.get_list_publis_ids_by_affid_sanslabo(
            df_affil_id, df_affil_org, row[0], row[1], start_year=row[2], end_year=row[3]
        )
        for row in zip(df_sans_labo['id'],df_sans_labo['aff_scopus_id'],df_sans_labo['harvest_start'], df_sans_labo['harvest_end'])
    ], axis=0)

# dataframe du triplet aff_internal_id/aff_scopus_id/dc_identifiers pour les structures UCA et OCA mentionnées ensemble (co-publis) sans labo
@asset
def get_sanslabo_uca_et_oca_affid_scopusid_pairs(df_affil_id, df_affil_org):
    return scp.get_list_publis_ids_by_affid_sanslabo_together(
            df_affil_id, df_affil_org
        )

@op(required_resource_keys={"config_params"})
def verify_result(context, df):
    context.log_event(
        AssetObservation(asset_key="verify_result", metadata={
            "text_metadata": 'affid-scopus id dataframe shape',
            "size": f'{df.shape}'})
        )
    return df

@op(required_resource_keys={"config_params"})
def concat_and_save(context,df1, df2, df3, df4):
    df = pd.concat([df1, df2, df3, df4], axis=0)
    df.to_csv(f'bso_publis/01_raw/{context.resources.config_params["observation_date"]}/scopus/harvested_corpus.csv', index=False, encoding="utf-8")

########################## JOBS ################################
################################################################

@job(name="01_create_subfolders",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("1. Création de l'architecture de dossiers nécessaire ; 2. NE PAS OUBLIER D'EXECUTER LE NOTEBOOK DE CONTROLE AVANT LES JOBS")
    }
)
def create_subfolders():
    create_subfolders()

@job(name="02_affid_scopusid_pairs",
    resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("")
    }
)
def affid_scopusid_pairs():
    df_referentiel_structures = get_referentiel_structures()
    df_affil_id = get_df_affil_id(df_referentiel_structures)
    df_affil_org = get_df_affil_org(df_referentiel_structures)
    df_affil_sanslabo = get_df_affil_sanslabo(df_referentiel_structures) # UCA et OCA
    df_affid_scopusid_pairs = get_affid_scopusid_pairs(df_affil_id)
    df_afforg_scopusid_pairs = get_afforg_scopusid_pairs(df_affil_org)
    df_affid_sanslabo_scopusid_pairs = get_sanslabo_affid_scopusid_pairs(df_affil_sanslabo, df_affil_id, df_affil_org)
    df_affid_sanslabo_uca_et_oca_affid_scopusid_pairs = get_sanslabo_uca_et_oca_affid_scopusid_pairs(df_affil_id, df_affil_org)
    concat_and_save(df_affid_scopusid_pairs, df_afforg_scopusid_pairs, df_affid_sanslabo_scopusid_pairs, df_affid_sanslabo_uca_et_oca_affid_scopusid_pairs)



@repository
def bso_publis_scopus():
    return [create_subfolders,affid_scopusid_pairs]

"""
resources:
  config_params:
    config:
      url_middleware_publications_prod : "https://api-scd.univ-cotedazur.fr/middleware-publications/api/v1"
      url_middleware_publications_local : "https://localhost:5003/publications-middleware/api/v1"
      observation_date: 2023-03-20
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


