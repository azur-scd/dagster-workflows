import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import glob
import helpers.functions as fn
import sqlite3

########################## 01_MAIN_FUZZY ################################

@op(required_resource_keys={"config_params"})
def extract_data_source(context):
    df = pd.read_json(f'{context.resources.config_params["raw_data_path"]}/{context.resources.config_params["observation_date"]}/exportDonnees_barometre_complet_{context.resources.config_params["observation_date"]}.json')
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
def transform_fuzzy_data_v2(df):
    df["fuzzy_uca_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_uca_developpee)
    df["fuzzy_uca_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_uca_sigle)
    df["fuzzy_uns"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_uns)
    return df

@op
def normalize_mention_adresse_v2(df):
    # mention adresse normalisée tous les cas
    df["mention_adresse_norm"] = np.nan
    df.loc[df['fuzzy_uca_developpee'] >= 90, 'mention_adresse_norm'] = 'uca_forme_developpee'
    df.loc[(df['fuzzy_uca_sigle'] >= 90) & (df['fuzzy_uca_developpee'] < 90), 'mention_adresse_norm'] = 'uca_sigle'
    df.loc[(df['fuzzy_uns'] >= 90) & (df['mention_adresse_norm'].isna()), 'mention_adresse_norm'] = 'uns'
    df.loc[df['mention_adresse_norm'].isna(), 'mention_adresse_norm'] = 'universite_non_mentionnee'
    # mention adresse normalisée 3 cas principaux de respect de la charte
    df["mention_adresse_norm_charte"] = np.nan
    df.loc[df['fuzzy_uca_developpee'] >= 90, 'mention_adresse_norm_charte'] = 'cas1_uca_forme_developpee'
    df.loc[(df['fuzzy_uca_sigle'] >= 90) & (df['fuzzy_uca_developpee'] < 90), 'mention_adresse_norm_charte'] = 'cas2_uca_sigle'
    df.loc[df['mention_adresse_norm_charte'].isna(), 'mention_adresse_norm_charte'] = 'cas3_autres'
    return df

@op(required_resource_keys={"config_params"})
def add_structures_aff_labels(context,df):
    df_affs = pd.read_json(f'{context.resources.config_params["primary_data_path"]}/referentiel_structures.json')
    df_affs['affiliation_id'] = df_affs['affiliation_id'].astype('string')
    df['@afids'] = df['@afids'].astype('string')
    df = pd.merge(df, df_affs[['affiliation_id', 'affiliation_name']], how='left', left_on=['@afids'], right_on=['affiliation_id']).drop(columns=['affiliation_id'])
    return df

@op
def regroup_mentions_adresse_by_publis(df):
    return fn.regroup(df,'mention_adresse_norm')

#ajout v2
@op
def regroup_mentions_adresse_charte_by_publis(df):
    return fn.regroup(df,'mention_adresse_norm_charte')

@op
def regroup_aff_ids_by_publis(df):
     return fn.regroup(df,'@afids')

@op
def regroup_aff_names_by_publis(df):
    return fn.regroup(df,'affiliation_name')

@op
def regroup_authors_names_by_publis(df):
    return fn.regroup(df,'ce:indexed-name')

@asset(required_resource_keys={"config_params"})
def save_detail_data(context,df):
    context.log_event(
        AssetObservation(asset_key="detail_controle_mentionAdresses", metadata={
            "text_metadata": 'Shape of the dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/detail_controle_mentionAdresses.csv', index=False, encoding="utf-8")

@op
def clean_regroup_by_publis_data_v2(df):
    # drop columns
    df = df.drop(columns=['@orcid','@auid','ce:indexed-name','mentionAffil_reconstruct', 'mentionAffil_reconstruct_subsentence_cleaned', 'fuzzy_uca_developpee', 'fuzzy_uca_sigle', 'fuzzy_uns', 'mention_adresse_norm', 'mention_adresse_norm_charte', '@afids', 'affiliation_name'])
    # prefered kepped rows when deduplicate
    for column_name in ['corresponding_author', 'Is_dc:creator']:
        df[column_name] = df[column_name].astype('category')
    df["corresponding_author"] = df["corresponding_author"].cat.set_categories(['oui', 'non', 'corr absent pour cette publi'], ordered=True)
    df["Is_dc:creator"] = df["Is_dc:creator"].cat.set_categories(['oui', 'non'], ordered=True)
    df.sort_values(by=['dc:identifiers', 'corresponding_author','Is_dc:creator'])
    df_deduplicate = df.drop_duplicates(subset=['dc:identifiers'], keep='first')
    # consolide mention_adresse tous les cas au niveau de la publication
    df_deduplicate.loc[df_deduplicate['regroup_mention_adresse_norm'].str.contains('uca_forme_developpee'),'synthese_mention_adresse_norm'] = 'uca_forme_developpee'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm'].str.contains('uca_sigle')) & (df_deduplicate['synthese_mention_adresse_norm'].isna()),'synthese_mention_adresse_norm'] = 'uca_sigle'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm'].str.contains('uns')) & (df_deduplicate['synthese_mention_adresse_norm'].isna()),'synthese_mention_adresse_norm'] = 'uns'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm'].str.contains('universite_non_mentionnee')) & (df_deduplicate['synthese_mention_adresse_norm'].isna()),'synthese_mention_adresse_norm'] = 'universite_non_mentionnee'
    # consolide mention_adresse 3 cas d'application de la charte au niveau de la publication
    df_deduplicate.loc[df_deduplicate['regroup_mention_adresse_norm_charte'].str.contains('cas1_uca_forme_developpee'),'synthese_mention_adresse_norm_charte'] = 'cas1_uca_forme_developpee'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm_charte'].str.contains('cas2_uca_sigle')) & (df_deduplicate['synthese_mention_adresse_norm_charte'].isna()),'synthese_mention_adresse_norm_charte'] = 'cas2_uca_sigle'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm_charte'].str.contains('cas3_autres')) & (df_deduplicate['synthese_mention_adresse_norm_charte'].isna()),'synthese_mention_adresse_norm_charte'] = 'cas3_autres'
    return df_deduplicate

@asset(required_resource_keys={"config_params"})
def save_regroup_by_publis_data(context,df):
    context.log_event(
        AssetObservation(asset_key="regroupbypublis_controle_mentionAdresses", metadata={
            "text_metadata": 'Shape of the dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/regroupbypublis_controle_mentionAdresses.csv', index=False, encoding="utf-8")

########################## 02_CONSOLIDATED_DATA ################################

@asset(required_resource_keys={"config_params"})
def get_detail_data(context):
    df = pd.read_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/detail_controle_mentionAdresses.csv',sep=",", encoding="utf-8")
    return df

@asset(required_resource_keys={"config_params"})
def get_regoup_data(context):
    df = pd.read_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/regroupbypublis_controle_mentionAdresses.csv',sep=",", encoding="utf-8")
    return df

@asset(required_resource_keys={"config_params"})
def consolidate_afids_value_counts(context,df):
    # from detail data
    ## dataframe value_counts par afids -> conversion en dict pour dcc.dropdown
    df_afids_value_counts = pd.DataFrame(df[["@afids","affiliation_name"]].value_counts()).reset_index().rename(columns={0: "total","@afids": "value"})
    df_afids_value_counts["label"] = df_afids_value_counts["affiliation_name"] + " (" + df_afids_value_counts["total"].astype(str) + ")"
    df_afids_value_counts[["label","value"]].to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/detail_afids_value_counts.csv', index=False, encoding="utf-8")

@asset(required_resource_keys={"config_params"})
def get_afids_value_counts(context):
    df = pd.read_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/detail_afids_value_counts.csv', sep=",", encoding="utf-8")
    return df

@asset(required_resource_keys={"config_params"})
def consolidate_regroup_crosstabs_v2(context,df):
    #from regroup data
    ##crosstab annee_pub/synthese_mention_adresse_norm en valeurs absolues avec totaux
    absolute_data = pd.crosstab(df["annee_pub"], df["synthese_mention_adresse_norm"],normalize=False, margins=True, margins_name="Total").reset_index()
    absolute_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_valeurs_absolues.csv', index=False, encoding="utf-8")
    ##crosstab annee_pub/synthese_mention_adresse_norm en pourcentages avec totaux
    percent_data = (pd.crosstab(df["annee_pub"], df["synthese_mention_adresse_norm"],normalize=True, margins=True, margins_name="Total")*100).round(3).reset_index()
    percent_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_pourcentages.csv', index=False, encoding="utf-8")
    ##crosstab annee_pub/synthese_mention_adresse_norm en indice base 100 en 2016 et sans totaux
    indice_data = absolute_data.iloc[:-1, :].iloc[:, :-1]
    for c in ['universite_non_mentionnee', 'uca_forme_developpee', 'uca_sigle', 'uns']:
        indice_data[f'indice_{c}'] = (indice_data[c].div(
            indice_data[c].iloc[0])*100).round(2)
    indice_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_indices.csv', index=False, encoding="utf-8")

########################## 03_SAVE_SQLITE ################################

@op(required_resource_keys={"config_params"})
def create_bsi_all_by_mention_adresse_table(context,df):
    df.columns = df.columns.str.replace('[:,@,-]', '_')
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bsi_all_by_mention_adresse_{observation_date}")
    cur.execute(f"CREATE TABLE bsi_all_by_mention_adresse_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"bsi_all_by_mention_adresse_{observation_date}", conn, if_exists='append', index=False)

@op(required_resource_keys={"config_params"})
def create_bsi_publis_uniques_table(context,df):
    df.columns = df.columns.str.replace('[:,@,-]', '_')
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bsi_publis_uniques_{observation_date}")
    cur.execute(f"CREATE TABLE bsi_publis_uniques_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"bsi_publis_uniques_{observation_date}", conn, if_exists='append', index=False)

@op(required_resource_keys={"config_params"})
def create_bsi_mention_count_by_afid_table(context,df):
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bsi_mention_count_by_afid_{observation_date}")
    cur.execute(f"CREATE TABLE bsi_mention_count_by_afid_{observation_date} ({','.join(map(str,df.columns))})")
    df.to_sql(f"bsi_mention_count_by_afid_{observation_date}", conn, if_exists='append', index=False)

########################## JOBS ################################
################################################################

@job(name="01_main_fuzzy_process",
     resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("1. Le fichier 08_reporting/DATE/controle_mention_adresse/toutes_mentions_affiliation.csv contient toutes les mentions d'affiliations avec ou sans DOI ; 2. Pas de sauvegarde intermédiaire en csv car la colonne mentionAffil_reconstruct_subset_cleaned serait sauvegardée en string ce qui fausse le fuzzy matching")
    }
)
def main_fuzzy_process():
    #configs
    data_source = extract_data_source()
    nlp_data = transform_nlp_data(data_source)
    fuzzy_data = transform_fuzzy_data_v2(nlp_data)
    norm_mention_adresse = normalize_mention_adresse_v2(fuzzy_data)
    structures_aff_labels = add_structures_aff_labels(norm_mention_adresse)
    mentions_adresse_by_publis = regroup_mentions_adresse_by_publis(structures_aff_labels)
    mentions_adresse_charte_by_publis = regroup_mentions_adresse_charte_by_publis(mentions_adresse_by_publis)
    aff_ids_by_publis = regroup_aff_ids_by_publis( mentions_adresse_charte_by_publis)
    aff_names_by_publis = regroup_aff_names_by_publis(aff_ids_by_publis)
    authors_names_by_publis = regroup_authors_names_by_publis(aff_names_by_publis)
    cleaned_regroup_by_publis_data = clean_regroup_by_publis_data_v2(authors_names_by_publis)
    save_detail_data(structures_aff_labels)
    save_regroup_by_publis_data(cleaned_regroup_by_publis_data)
    
@job(name="02_consolidated_data",
resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("")
    }
)
def consolidated_data():
    detail_data = get_detail_data()
    regroup_data = get_regoup_data()
    consolidate_afids_value_counts(detail_data)
    consolidate_regroup_crosstabs_v2(regroup_data)

@job(name="03_save_sqlite",
resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("")
    }
)
def save_sqlite():
    detail_data = get_detail_data()
    regroup_data = get_regoup_data()
    afids_value_counts = get_afids_value_counts()
    create_bsi_all_by_mention_adresse_table(detail_data)
    create_bsi_publis_uniques_table(regroup_data)
    create_bsi_mention_count_by_afid_table(afids_value_counts)


@repository
def prod_bso_publis_scopus():
    return [main_fuzzy_process,consolidated_data,save_sqlite]

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
      corpus_end_year: 2022
"""


