import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import requests
import sqlite3
import s3fs
import os
import helpers.doaj_harvest as doaj

########################## 01_CREATE_SUBFOLDER APC ################################

@op(required_resource_keys={"config_params"})
def create_subfolder(context):
    if not os.path.exists(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc'):
            os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc')
            os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation')  
            os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca') 
            os.mkdir(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/bso_apc') 

########################## 02_GET_DATA ################################
 

@op(required_resource_keys={"config_params"})
def minio_connection(context):
    return s3fs.S3FileSystem(
        client_kwargs={
            'endpoint_url': context.resources.config_params["S3_ENDPOINT_URL"]
        },
        key=context.resources.config_params["AWS_ACCESS_KEY_ID"],
        secret=context.resources.config_params["AWS_SECRET_ACCESS_KEY"],
        token=context.resources.config_params["AWS_SESSION_TOKEN"],
    )

@asset(required_resource_keys={"config_params"})
def get_bso_apc_data(context,fs):
    with fs.open(f'{context.resources.config_params["BUCKET"]}/{context.resources.config_params["APC_FOLDER_KEY_S3"]}/{context.resources.config_params["APC_FILE_KEY_S3"]}', mode="rb") as file_in:
        df = pd.read_csv(file_in, sep=",", encoding="utf-8")
    context.log_event(
        AssetObservation(asset_key="minio_bso_apc_dataset", metadata={
            "text_metadata": 'Shape of the BSO APC dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

@asset(required_resource_keys={"config_params"})
def get_publis_uniques_doi_oa_data_with_bsoclasses_complete(context):
    df= pd.read_csv(
        f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/open_access/publis_uniques_doi_oa_data_with_bsoclasses_complete.csv',
        sep=",",
        encoding="utf-8",
    )
    context.log_event(
        AssetObservation(asset_key="bso_uca_dataset", metadata={
            "text_metadata": 'Shape of the BSO UCA dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

########################## 03_PROCESS_DATA (MERGE)################################

@op
def process_bso_apc(df):
    # On ne filtre pas tout de suite sur l'année de publi afin de garder le dataset entier pour l'étape du merge
    df = df.drop(columns=['bso_classification', 'coi', 'email','detected_countries', 'journal_issn_l', 'lang', 'title', 'hal_id', 'author_position', 'nb_authors', 'is_complete_affiliation', 'nb_missing_affiliation', 'is_french_CA', 'is_at_least_one_french_author2', 'oa_details.2020.is_oa', 'oa_details.2020.journal_is_in_doaj', 'oa_details.2020.journal_is_oa', 'oa_details.2020.oa_host_type', 'oa_details.2020.oa_colors', 'oa_details.2020.oa_colors_with_priority_to_publisher', 'oa_details.2020.licence_publisher', 'is_french_CA_wos', 'is_non_french_CA_wos', 'is_french_CA_openalex', 'is_french_CA_bso_wos', 'is_french_CA_bso_wos_openalex_single_lang', 'is_french_CA_bso_wos_openalex', 'is_french_CA_bso_wos_openalex_single'])
    # Step: Change data type of ['year', 'has_apc'] to String/Text
    for column_name in ['year', 'apc_has_been_paid','has_apc']:
        df[column_name] = (df[column_name]
                           .astype('string')
                           .str.replace('.0', '', regex=False)
                          )
    # Step: Replace missing values in corresponding variable
    df[['corresponding']] = df[['corresponding']].fillna(False) 
    return df

@op
def process_bso_uca(context,df):
    df['year'] = df['year'].astype('string')
    # Step: Keep rows where year is one of: 2016, 2017, 2018, 2019, 2020
    df = df.loc[df['year'].isin(['2016', '2017', '2018', '2019', '2020'])]
    # Step: Keep rows where genre is one of: journal-article
    df = df.loc[df['genre'].isin(['journal-article'])]
    # Step: Copy a dataframe column
    df['corresponding_normalized'] = df['corresponding']
    # Step: Set values of corresponding_normalized to non where corresponding is missing and otherwise to oui
    tmp_condition = df['corresponding'].isna()
    df.loc[tmp_condition, 'corresponding_normalized'] = 'non'
    df.loc[~tmp_condition, 'corresponding_normalized'] = 'oui'
    df = df[['source_id', 'doi', 'year', 'corresponding', 'all_authors', 'corresponding_normalized','title', 'journal_name',
       'journal_issn_l', 'journal_is_oa', 'journal_is_in_doaj','is_oa_normalized', 'oa_status_normalized','oa_host_type_normalized','oa_repo_normalized','publisher_by_doiprefix','bso_classification_fr','funder']]
    context.log_event(
        AssetObservation(asset_key="processed bso_uca_dataset", metadata={
            "text_metadata": 'Shape of the processed BSO UCA dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df.add_prefix('sourceuca_')

@asset(required_resource_keys={"config_params"})
def calculate_recouvrement(context,df_bso_apc,df_uca):
    list_apc_doi = df_bso_apc["doi"].tolist()
    df_missing_doi = df_uca[~df_uca['sourceuca_doi'].isin(list_apc_doi)]
    context.log_event(
        AssetObservation(asset_key="calculated recouvrement rate", metadata={
            "text_metadata": 'Percent of UCA articles that are not in the BSO APC dataframe',
            "size": f"{(round(df_missing_doi.shape[0] / df_uca.shape[0]) * 100,1)}% are not covered by the BSO APC dataset"})
    )
    df_missing_doi.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/missing_uca_doi_in_apc_data.csv',index = False,encoding='utf8')
    return df_missing_doi

@asset(required_resource_keys={"config_params"})
def merge_dfs(context,df_bso_apc,df_uca):
    # Step: Remove leading and trailing whitespaces in 'doi' for df_uca
    df_uca['sourceuca_doi'] = df_uca['sourceuca_doi'].str.strip()
    # Step: Remove leading and trailing whitespaces in 'doi' for df_bso_apc
    df_bso_apc['doi'] = df_bso_apc['doi'].str.strip()
    df = pd.merge(df_uca, df_bso_apc, left_on="sourceuca_doi", right_on="doi", how="left").drop(columns={'doi','year'})
    context.log_event(
        AssetObservation(asset_key="merged dataset", metadata={
            "text_metadata": 'Shape of the merged dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/articles_with_apc.csv',index = False,encoding='utf8')
    return df

########################## 04_GENERATE_CONSOLIDATED_DATA ################################

def get_crosstab(df, col1, col2):
    return pd.crosstab(df[col1], df[col2],margins=True, margins_name='total').reset_index()

def get_crosstab_percent(df, col1, col2):
    return (pd.crosstab(df[col1], df[col2],margins=True, margins_name='total', normalize=True)*100).round(1).reset_index()

@asset(required_resource_keys={"config_params"})
def filter_bso_apc(context,df):
    #maintenant on peut filtrer sur l'année de publi et ne garder que 2016-2020
    # Step: Change data type of year to String/Text
    df['year'] = df['year'].astype('string')
    # Step: Keep rows where year is one of: 2016, 2017, 2018, 2019, 2020
    df = df.loc[df['year'].isin(['2016', '2017', '2018', '2019', '2020'])]
    context.log_event(
        AssetObservation(asset_key="filtered bso_apc_dataset", metadata={
            "text_metadata": 'Shape of the filtered BSO APC dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/filtered_2016_2020_national_apc_data.csv',index = False,encoding='utf8')
    return df

@asset(required_resource_keys={"config_params"})
def reconstruct_uca_count_corr_by_year(context,df):
    """
    This function takes a dataframe and a context object as input, and outputs two csv files, one with
    the count of the number of times a uca was corrected by year, and the other with the percentage of
    times a uca was corrected by year
    
    :param context: the context object that is passed to the function
    :param df: the dataframe to be used
    """
    c = get_crosstab(df,'year','corresponding_normalized')
    c_percent = get_crosstab_percent(df,'sourceuca_year','sourceuca_corresponding_normalized')
    c.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca/count_corr_by_year.csv',index = False,encoding='utf8')
    c_percent.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca/percent_corr_by_year.csv',index = False,encoding='utf8')

@asset(required_resource_keys={"config_params"})
def reconstruct_bso_apc_count_corr_by_year(context,df):
    c = get_crosstab(df,'year','corresponding_normalized')
    c_percent = get_crosstab_percent(df,'year','corresponding_normalized')
    c.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/bso_apc/count_corr_by_year.csv',index = False,encoding='utf8')
    c_percent.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/bso_apc/percent_corr_by_year.csv',index = False,encoding='utf8')

@asset(required_resource_keys={"config_params"})
def reconstruct_uca_count_corr_by_oacolor(context,df):
    c = get_crosstab(df,'oa_color_finale','sourceuca_corresponding_normalized')
    c_percent = get_crosstab_percent(df,'oa_color_finale','sourceuca_corresponding_normalized')
    c.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca/count_corr_by_oa_color.csv',index = False,encoding='utf8')
    c_percent.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca/percent_corr_by_oa_color.csv',index = False,encoding='utf8')

@asset(required_resource_keys={"config_params"})
def reconstruct_bso_apc_count_corr_by_oa_color(context,df):
    c = get_crosstab(df,'oa_color_finale','corresponding_normalized')
    c_percent = get_crosstab_percent(df,'oa_color_finale','corresponding_normalized')
    c.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/bso_apc/count_corr_by_oa_color.csv',index = False,encoding='utf8')
    c_percent.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/bso_apc/percent_corr_by_color.csv',index = False,encoding='utf8')

@asset(required_resource_keys={"config_params"})
def reconstruct_uca_count_corr_by_classification(context,df):
    df_grouped = df.groupby(['sourceuca_bso_classification_fr', 'sourceuca_corresponding_normalized']).agg(doi_size=('sourceuca_doi', 'size')).reset_index()
    df_grouped['%'] = 100 * df_grouped['doi_size'] / df_grouped.groupby('sourceuca_bso_classification_fr')['sourceuca_doi_size'].transform('sum')
    df_grouped.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca/count_corr_by_classification.csv',index = False,encoding='utf8')
    return df_grouped

@asset(required_resource_keys={"config_params"})
def reconstruct_bso_apc_count_corr_by_classification(context,df):
    df_grouped = df.groupby(['bso_classification_fr', 'corresponding_normalized']).agg(doi_size=('doi', 'size')).reset_index()
    df_grouped['%'] = 100 * df_grouped['doi_size'] / df_grouped.groupby('bso_classification_fr')['doi_size'].transform('sum')
    df_grouped.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/consolidation/uca/count_corr_by_classification.csv',index = False,encoding='utf8')
    return df_grouped

########################## 04_POSTPROCESS_DATA (ENRICH)################################
#Sur le merge simple, 20% des plus de 9000 publis UCA ne sont pas présentes dans le dataset BSO APC national, et 76% de ces 80% sont de type OA gold ou hybrid d'après le BSO UCA (donc Unpaywall)#
#Donc post completion des estimations APC sur ces publis, au moins sur le gold en requêtant le DOAJ#

########################## 05_SAVE_SQLITE ################################

@asset(required_resource_keys={"config_params"})
def get_bso_uca_apc(context):
    return pd.read_csv(
        f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/articles_with_apc.csv',
        sep=",",
        encoding="utf-8",
    )

@asset(required_resource_keys={"config_params"})
def get_national_filtered_apc(context):
    return pd.read_csv(
        f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/apc/filtered_2016_2020_national_apc_data.csv',
        sep=",",
        encoding="utf-8",
    )

@op(required_resource_keys={"config_params"})
def db_connexion(context):
    return sqlite3.connect(f'{context.resources.config_params["db_path"]}')

@op(required_resource_keys={"config_params"})
def create_bso_uca_with_apc_table(context,df):
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS bso_articles_with_apc_{observation_date}")
    cur.execute(f"CREATE TABLE bso_articles_with_apc_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"bso_articles_with_apc_{observation_date}", conn, if_exists='append', index=False)

@op(required_resource_keys={"config_params"})
def create_national_apc_table(context,df):
    observation_date = context.resources.config_params['observation_date'].replace("-","")
    conn = sqlite3.connect(f'{context.resources.config_params["db_path"]}')
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS national_with_apc_{observation_date}")
    cur.execute(f"CREATE TABLE national_with_apc_{observation_date} ({','.join(map(str,df.columns))});")
    return df.to_sql(f"national_with_apc_{observation_date}", conn, if_exists='append', index=False)

########################## JOBS ################################
################################################################

@job(name="01_create_apc_subfolders",
     resource_defs={"config_params": make_values_resource()},
     metadata={
        "notes": MetadataValue.text("1. Création de l'architecture de dossiers nécessaire dans /08_reporting")
    }
)
def create_subfolders():
    create_subfolder()

@job(name="02_get_data",
     resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("1. Le dataset du BSO APC national est stocké sur le serveur Minio du SSP Cloud ; 2. NE ÄS OUBLIER DE COMPLETER LES KEY ID, ACCESS KEY ET TOKEN DANS LA CONFIGURATION")
    }
)
def get_and_process_data():
    #configs
    fs = minio_connection()
    bso_apc_data = get_bso_apc_data(fs)
    bso_uca_data = get_publis_uniques_doi_oa_data_with_bsoclasses_complete()
    filtered_bso_apc = process_bso_apc(bso_apc_data)
    filtered_bso_uca = process_bso_uca(bso_uca_data)
    merge_dfs(filtered_bso_apc,filtered_bso_uca)

@job(name="03_save_sqlite",
resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("")
    }
)
def save_sqlite():
    bso_uca_apc = get_bso_uca_apc()
    create_bso_uca_with_apc_table(bso_uca_apc)
    national_apc = get_national_filtered_apc()
    create_national_apc_table(national_apc)


@repository
def bso_publis_apc():
    return [create_subfolders,get_and_process_data,save_sqlite]

"""
resources:
  config_params:
    config:
      observation_date: 2022-12-31
      reporting_data_path: bso_publis_scopus/08_reporting
      db_path: bso_publis_scopus/09_db/apc.db
      S3_ENDPOINT_URL: https://minio.lab.sspcloud.fr
      AWS_ACCESS_KEY_ID: "JABZ84TYE0YW5FIO7Y7N"
      AWS_SECRET_ACCESS_KEY: "yeCCpEDP46jGho+KpygBhijBBacENHHduOHB4mgs"
      AWS_SESSION_TOKEN: 
      BUCKET: "ggeoffroy"
      APC_FOLDER_KEY_S3: "etude-apc/datactivist-etude-apc/dataverse_files/input"
      APC_FILE_KEY_S3: "BSO_final_reconstructed.csv"
"""


