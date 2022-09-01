import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import helpers as hp
import os

@op(config_schema={"raw_data_path": str, "intermediate_data_path": str, "primary_data_path": str, "datagouv_abes_dataset_url": str, "last_observation_date": str, "uca_etabs": list})
def get_config(context):
    return context.op_config

@op
def create_subdirectory(config_params):
    last_observation_date = config_params["last_observation_date"]
    if not os.path.exists(f'{config_params["primary_data_path"]}/{last_observation_date}'):
        os.mkdir(f'{config_params["primary_data_path"]}/{last_observation_date}')

@asset
def load_abes_datagouv_data(context):
    df = pd.read_csv("https://www.data.gouv.fr/fr/datasets/r/eb06a4f5-a9f1-4775-8226-33425c933272",sep=",", encoding="utf-8")
    context.log_event(
        AssetObservation(asset_key="abes_datagouv_data", metadata={
            "text_metadata": "Shape of the source Abes file on data.gouv.fr",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

@op
def extract_uca_data(config_params,df):
    df_tmp = df[df.source == "star"][['accessible', 'auteurs.0.idref', 'auteurs.0.nom', 'auteurs.0.prenom', 'cas', 'code_etab', 'date_soutenance', 'directeurs_these.0.idref', 'directeurs_these.0.nom', 'directeurs_these.0.prenom', 'directeurs_these.1.idref', 'directeurs_these.1.nom', 'directeurs_these.1.prenom', 'discipline.fr', 'ecoles_doctorales.0.nom', 'embargo', 'etablissements_soutenance.0.idref', 'etablissements_soutenance.1.idref', 'etablissements_soutenance.0.nom', 'etablissements_soutenance.1.nom', 'iddoc', 'langue', 'nnt', 'oai_set_specs', 'partenaires_recherche.0.idref', 'partenaires_recherche.0.nom', 'partenaires_recherche.0.type', 'partenaires_recherche.1.idref', 'partenaires_recherche.1.nom', 'partenaires_recherche.1.type', 'partenaires_recherche.2.idref', 'partenaires_recherche.2.nom', 'partenaires_recherche.2.type','titres.fr']]
    etabs_uca = config_params["uca_etabs"]
    appended_data = []
    for i in etabs_uca:
        data = df_tmp[df_tmp["code_etab"] == str(i)]
        appended_data.append(data)
    appended_data = pd.concat(appended_data)
    return appended_data.convert_dtypes()

@op
def extract_fr_data(df):
    df_tmp = df[df.source == "star"][['accessible', 'cas', 'code_etab', 'date_soutenance','discipline.fr','etablissements_soutenance.1.nom','embargo','langue', 'nnt', 'oai_set_specs']]
    return df_tmp.convert_dtypes()

@asset
def scrap_oai_data(context,config_params):
    df = hp.scrapping_oai_sets_dewey()
    context.log_event(
        AssetObservation(asset_key="scrapped_oai_sets", metadata={
            "text_metadata": "Shape of the scrapped mapping between oai_set_specs labels and Dewey classes",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df.to_csv(f'{config_params["intermediate_data_path"]}/oai_set_specs_dewey_labels.csv', index=False, encoding='utf8')
    return df.to_dict('records')

@op
def clean_column_names(df):
    df = df.set_axis([w.replace('.', '_') for w in df.columns], axis=1, inplace=False)
    return df

@op
def clean_ending_data(df):
    for column_name in df.columns:
        df[column_name] = df[column_name].astype('string')
        if column_name == "ecoles_doctorales_0_nom":
            df[['ecoles_doctorales_0_nom']] = df[['ecoles_doctorales_0_nom']].fillna('non renseigné')
    df['embargo_duree'] = pd.to_numeric(df['embargo_duree'], downcast='integer', errors='coerce')
    df = df.fillna('')
    return df

@op
def create_oa_variables(df):
    df["is_oa_normalized"] = df["accessible"]
    df['is_oa_normalized'] = df['is_oa_normalized'].replace(['oui','non'],['Accès ouvert','Accès fermé'])
    return df

@op
def create_date_variables(df):
    df["annee_civile_soutenance"] = df['date_soutenance'].apply(lambda x: x.split("-")[0])
    df["mois_soutenance"] = df['date_soutenance'].apply(lambda x: x.split("-")[1])
    df["annee_univ_soutenance"] = df.apply(lambda row: hp.calculate_annee_civile(row), axis=1)
    return df

@op
def create_embargo_variables(df):
    df["embargo_duree"] = df.apply(lambda row: hp.days_between(row), axis=1)
    df[['embargo_duree']] = df[['embargo_duree']].fillna(0)
    df['has_exist_embargo'] = df['embargo']
    tmp_condition = df['has_exist_embargo'].isna()
    df.loc[tmp_condition, 'has_exist_embargo'] = 'non'
    df.loc[~tmp_condition, 'has_exist_embargo'] = 'oui'
    return df

@op
def create_discipline_variables(df,oai_data):
    #split oai_sets_specs in two columns for the multivalues
    split_df = df['oai_set_specs'].str.split('\|\|', expand=True)
    split_df.columns = ['oai_set_specs' + f"_{id_}" for id_ in range(len(split_df.columns))]
    df = pd.merge(df, split_df, how="left", left_index=True, right_index=True)
    df = df.drop(columns=['oai_set_specs_2', 'oai_set_specs_3'])
    #new cols with disc label 
    df['oai_set_specs_0_label'] = df['oai_set_specs_0'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    df['oai_set_specs_1_label'] = df[df['oai_set_specs_1'].notna()]['oai_set_specs_1'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    #new cols (dcc-codes and labels) with disc regroup in main dewey classes 0XX, 1XX, 2XX etc...
    df["oai_set_specs_0_regroup"] = df['oai_set_specs_0'].apply(lambda x : x[0:5]+"00")
    df['oai_set_specs_0_regroup_label'] = df['oai_set_specs_0_regroup'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    df["oai_set_specs_1_regroup"] = df[df['oai_set_specs_1'].notna()]['oai_set_specs_1'].apply(lambda x : x[0:5]+"00")
    df['oai_set_specs_1_regroup_label'] = df[df['oai_set_specs_1_regroup'].notna()]['oai_set_specs_1_regroup'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    #new col (only based on oai_set_specs_0) with disc regroup in main domains 'Sciences, Technologies, Santé' and 'Lettres, sciences Humaines et Sociales'
    df['oai_set_specs_0_main_domain'] = df['oai_set_specs_0'].apply(lambda x: [y['main_domain'] for y in oai_data if y['code'] == str(x)][0])
    return df

@asset
def save_uca_data(context,config_params,df):
    context.log_event(
        AssetObservation(asset_key="result_uca_dataset", metadata={
            "text_metadata": 'Shape of the UCA result dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df.to_csv(f'{config_params["primary_data_path"]}/{config_params["last_observation_date"]}/theses_uca_processed.csv', index=False, encoding='utf8')
    return df

@asset
def save_fr_data(context,config_params, df):
    context.log_event(
        AssetObservation(asset_key="result_fr_dataset", metadata={
            "text_metadata": 'Shape of the fr result dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df.to_csv(f'{config_params["primary_data_path"]}/{config_params["last_observation_date"]}/theses_fr_processed.csv', index=False, encoding='utf8')
    return df

@graph
def run_workflow():
    #configs
    config_params = get_config()
    create_subdirectory(config_params)
    #get oai/dewey mapping
    load_oai_data = scrap_oai_data(config_params)
    #get source datafile
    load_abes_data = load_abes_datagouv_data()
    #process uca data
    save_uca_data(config_params,clean_ending_data(create_discipline_variables(create_embargo_variables(create_date_variables(create_oa_variables(clean_column_names(extract_uca_data(config_params,load_abes_data))))),load_oai_data)))
    #process fr data
    save_fr_data(config_params,clean_ending_data(create_discipline_variables(create_embargo_variables(create_date_variables(create_oa_variables(extract_fr_data(load_abes_data)))),load_oai_data)))
    #todo : process udice data

@repository
def prod_bso_theses_workflow():
    return [run_workflow]