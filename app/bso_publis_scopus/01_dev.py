import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import glob
import helpers.functions as fn
import sqlite3

########################## [OLD] 01_MAIN_FUZZY ################################

@op
def transform_fuzzy_data_v1(df):
    df["fuzzy_extractone_uca_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uca_developpee)
    df["fuzzy_extractone_uca_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uca_sigle)
    df["fuzzy_extractone_uns_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uns_developpee)
    df["fuzzy_extractone_uns_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uns_sigle)
    return df

@op
def normalize_mention_adresse_v1(df):
    df["mention_adresse_norm"] = np.nan
    df.loc[df['fuzzy_extractone_uca_developpee'] >= 90, 'mention_adresse_norm'] = 'uca_developpee'
    df.loc[(df['fuzzy_extractone_uca_sigle'] >= 90) & (df['fuzzy_extractone_uca_developpee'] < 90), 'mention_adresse_norm'] = 'uca_sigle_seul'
    df.loc[((df['fuzzy_extractone_uns_developpee'] >= 90) | (df['fuzzy_extractone_uns_sigle'] >= 90)) & (df['mention_adresse_norm'].isna()), 'mention_adresse_norm'] = 'uns_seul'
    df.loc[df['mention_adresse_norm'].isna(), 'mention_adresse_norm'] = 'ni_uca_ni_uns'
    return df

@op
def clean_regroup_by_publis_data(df):
    # drop columns
    df = df.drop(columns=['@orcid','@auid','ce:indexed-name','mentionAffil_reconstruct', 'mentionAffil_reconstruct_subsentence_cleaned', 'fuzzy_extractone_uca_developpee', 'fuzzy_extractone_uca_sigle', 'fuzzy_extractone_uns_developpee', 'fuzzy_extractone_uns_sigle', 'mention_adresse_norm', '@afids', 'affiliation_name'])
    # prefered kepped rows when deduplicate
    for column_name in ['corresponding_author', 'Is_dc:creator']:
        df[column_name] = df[column_name].astype('category')
    df["corresponding_author"] = df["corresponding_author"].cat.set_categories(['oui', 'non', 'corr absent pour cette publi'], ordered=True)
    df["Is_dc:creator"] = df["Is_dc:creator"].cat.set_categories(['oui', 'non'], ordered=True)
    df.sort_values(by=['dc:identifiers', 'corresponding_author','Is_dc:creator'])
    df_deduplicate = df.drop_duplicates(subset=['dc:identifiers'], keep='first')
    # consolide mention_adresse at publi level
    df_deduplicate.loc[df_deduplicate['regroup_mention_adresse_norm'].str.contains('uca_developpee'),'synthese_mention_adresse_norm'] = 'uca_developpee'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm'].str.contains('uca_sigle_seul')) & (df_deduplicate['synthese_mention_adresse_norm'].isna()),'synthese_mention_adresse_norm'] = 'uca_sigle_seul'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm'].str.contains('uns_seul')) & (df_deduplicate['synthese_mention_adresse_norm'].isna()),'synthese_mention_adresse_norm'] = 'uns_seul'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm'].str.contains('ni_uca_ni_uns')) & (df_deduplicate['synthese_mention_adresse_norm'].isna()),'synthese_mention_adresse_norm'] = 'ni_uca_ni_uns'
    return df_deduplicate

@asset(required_resource_keys={"config_params"})
def consolidate_regroup_crosstabs(context,df):
    #from regroup data
    ##crosstab annee_pub/synthese_mention_adresse_norm en valeurs absolues avec totaux
    absolute_data = pd.crosstab(df["annee_pub"], df["synthese_mention_adresse_norm"],normalize=False, margins=True, margins_name="Total").reset_index()
    absolute_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_valeurs_absolues.csv', index=False, encoding="utf-8")
    ##crosstab annee_pub/synthese_mention_adresse_norm en pourcentages avec totaux
    percent_data = (pd.crosstab(df["annee_pub"], df["synthese_mention_adresse_norm"],normalize=True, margins=True, margins_name="Total")*100).round(3).reset_index()
    percent_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_pourcentages.csv', index=False, encoding="utf-8")
    ##crosstab annee_pub/synthese_mention_adresse_norm en indice base 100 en 2016 et sans totaux
    indice_data = absolute_data.iloc[:-1, :].iloc[:, :-1]
    for c in ['ni_uca_ni_uns', 'uca_developpee', 'uca_sigle_seul', 'uns_seul']:
        indice_data[f'indice_{c}'] = (indice_data[c].div(
            indice_data[c].iloc[0])*100).round(2)
    indice_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_indices.csv', index=False, encoding="utf-8")

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


