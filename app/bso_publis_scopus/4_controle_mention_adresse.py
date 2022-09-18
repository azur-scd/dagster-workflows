import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue
import pandas as pd
import numpy as np
import glob
import helpers.functions as fn

@op(config_schema={"raw_data_path": str, "observation_date": str})
def extract_data_source(context):
    df = pd.read_json(f'{context.op_config["raw_data_path"]}/{context.op_config["observation_date"]}/exportDonnees_barometre_complet_{context.op_config["observation_date"]}.json')
    context.log_event(
        AssetObservation(asset_key="initial_jp_dataset", metadata={
            "text_metadata": 'Shape of the initial JP dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    # keep columns
    df = df[["dc:identifiers","prism:doi","reference","annee_pub","@afids","mentionAffil_reconstruct","@auid","ce:indexed-name", '@orcid',"corresponding_author","Is_dc:creator"]]
    return df

@op
def transform_nlp_data(df):
    df['mentionAffil_reconstruct_subsentence_cleaned'] = df['mentionAffil_reconstruct'].apply(fn.str2list).apply(fn.list2str)
    return df


@op
def transform_fuzzy_data(df):
    df["fuzzy_extractone_uca_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uca_developpee)
    df["fuzzy_extractone_uca_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uca_sigle)
    df["fuzzy_extractone_uns_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uns_developpee)
    df["fuzzy_extractone_uns_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uns_sigle)
    return df

@op
def normalize_mention_adresse(df):
    df["mention_adresse_norm"] = np.nan
    df.loc[df['fuzzy_extractone_uca_developpee'] >= 90, 'mention_adresse_norm'] = 'uca_developpee'
    df.loc[(df['fuzzy_extractone_uca_sigle'] >= 90) & (df['fuzzy_extractone_uca_developpee'] < 90), 'mention_adresse_norm'] = 'uca_sigle_seul'
    df.loc[((df['fuzzy_extractone_uns_developpee'] >= 90) | (df['fuzzy_extractone_uns_sigle'] >= 90)) & (df['mention_adresse_norm'].isna()), 'mention_adresse_norm'] = 'uns_seul'
    df.loc[df['mention_adresse_norm'].isna(), 'mention_adresse_norm'] = 'ni_uca_ni_uns'
    return df

@op(config_schema={"primary_data_path": str})
def add_structures_aff_labels(context,df):
    df_affs = pd.read_json(f'{context.op_config["primary_data_path"]}/referentiel_structures.json')
    df_affs['affiliation_id'] = df_affs['affiliation_id'].astype('string')
    df['@afids'] = df['@afids'].astype('string')
    df = pd.merge(df, df_affs[['affiliation_id', 'affiliation_name']], how='left', left_on=['@afids'], right_on=['affiliation_id']).drop(columns=['affiliation_id'])
    return df

@op
def regroup_mentions_adresse_by_publis(df):
    return fn.regroup(df,'mention_adresse_norm')

@op
def regroup_aff_ids_by_publis(df):
     return fn.regroup(df,'@afids')

@op
def regroup_aff_names_by_publis(df):
    return fn.regroup(df,'affiliation_name')

@op
def regroup_authors_names_by_publis(df):
    return fn.regroup(df,'ce:indexed-name')

@asset(config_schema={"reporting_data_path": str, "observation_date": str})
def save_detail_data(context,df):
    context.log_event(
        AssetObservation(asset_key="detail_controle_mentionAdresses", metadata={
            "text_metadata": 'Shape of the dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df.to_csv(f'{context.op_config["reporting_data_path"]}/{context.op_config["observation_date"]}/controle_mention_adresse/detail_controle_mentionAdresses.csv', index=False, encoding="utf-8")

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

@asset(config_schema={"reporting_data_path": str, "observation_date": str})
def save_regroup_by_publis_data(context,df):
    context.log_event(
        AssetObservation(asset_key="regroupbypublis_controle_mentionAdresses", metadata={
            "text_metadata": 'Shape of the dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df.to_csv(f'{context.op_config["reporting_data_path"]}/{context.op_config["observation_date"]}/controle_mention_adresse/regroupbypublis_controle_mentionAdresses.csv', index=False, encoding="utf-8")

@job(
    metadata={
        "notes": MetadataValue.text("1. Le fichier 08_reporting/DATE/controle_mention_adresse/toutes_mentions_affiliation.csv contient toutes les mentions d'affiliations avec ou sans DOI ; 2. Pas de sauvegarde intermédiaire en csv car la colonne mentionAffil_reconstruct_subset_cleaned serait sauvegardée en string ce qui fausse le fuzzy matching")
    }
)
def controle_mention_adresse():
    #configs
    data_source = extract_data_source()
    nlp_data = transform_nlp_data(data_source)
    fuzzy_data = transform_fuzzy_data(nlp_data)
    norm_mention_adresse = normalize_mention_adresse(fuzzy_data)
    structures_aff_labels = add_structures_aff_labels(norm_mention_adresse)
    mentions_adresse_by_publis = regroup_mentions_adresse_by_publis(structures_aff_labels)
    aff_ids_by_publis = regroup_aff_ids_by_publis(mentions_adresse_by_publis)
    aff_names_by_publis = regroup_aff_names_by_publis(aff_ids_by_publis)
    authors_names_by_publis = regroup_authors_names_by_publis(aff_names_by_publis)
    cleaned_regroup_by_publis_data = clean_regroup_by_publis_data(authors_names_by_publis)
    save_detail_data(structures_aff_labels)
    save_regroup_by_publis_data(cleaned_regroup_by_publis_data)
    


@repository
def prod_bso_publis_scopus():
    return [controle_mention_adresse]

"""
Config
ops:
  extract_data_source:
    config:
      observation_date: 2022-08-29
      raw_data_path: bso_publis_scopus/01_raw
  add_structures_aff_labels:
    config:
      primary_data_path: bso_publis_scopus/03_primary
  save_detail_data:
    config:
      observation_date: 2022-08-29
      reporting_data_path: bso_publis_scopus/08_reporting
  save_regroup_by_publis_data:
    config:
      observation_date: 2022-08-29
      reporting_data_path: bso_publis_scopus/08_reporting
"""


