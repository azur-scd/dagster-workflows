import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import glob
import helpers.functions as fn
import sqlite3
from sentence_transformers import SentenceTransformer, util

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
def get_fuzzy_data_uca_dvp(df):
    df["fuzzy_data_uca_dvp"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.process_fuzzy_score_uca_developpee)
    df["fuzzy_score_uca_dvp"] = df["fuzzy_data_uca_dvp"].apply(lambda x: x[1])
    df["fuzzy_position_uca_dvp"] = df.apply(lambda x: fn.process_uca_position(x["fuzzy_data_uca_dvp"],x["mentionAffil_reconstruct_subsentence_cleaned"]), axis=1)
    return df

@op
def get_fuzzy_score_uca_sigle(df):
    df["fuzzy_score_uca_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(lambda x: fn.process_fuzzy_sigle(x," uca "))
    return df

@op
def get_fuzzy_data_uns(df):
    df["fuzzy_score_uns"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(lambda x: fn.process_fuzzy_score_uns(x))
    return df

@op
def normalize_mention_adresse(df):
    # ajout d'une col : mention_adresse_norm cas général
    df["mention_adresse_norm"] = np.nan
    df.loc[df['fuzzy_score_uca_dvp'] >= 87, 'mention_adresse_norm'] = 'uca_forme_developpee'
    df.loc[(df['fuzzy_score_uca_sigle'] >= 90) & (df['mention_adresse_norm'].isna()), 'mention_adresse_norm'] = 'uca_sigle'
    df.loc[((df['fuzzy_score_uns'] >= 90)) & (df['mention_adresse_norm'].isna()), 'mention_adresse_norm'] = 'uns'
    df.loc[df['mention_adresse_norm'].isna(), 'mention_adresse_norm'] = 'universite_non_mentionnee'
    return df
	
@op(required_resource_keys={"config_params"})
def normalize_mention_adresse_avec_position(context,df):
    # ajout d'une col : mention_adresse_norm_position détail position uca
    df["mention_adresse_norm_avec_position"] = np.nan	
    df.loc[(df['mention_adresse_norm']  == "uca_forme_developpee") & (df['fuzzy_position_uca_dvp'] == "debut"), 'mention_adresse_norm_avec_position'] = 'uca_forme_developpee_debut'
    df.loc[(df['mention_adresse_norm']  == "uca_forme_developpee") & (df['fuzzy_position_uca_dvp'] == "interne"), 'mention_adresse_norm_avec_position'] = 'uca_forme_developpee_interne'
    df.loc[df['mention_adresse_norm']  == "uca_sigle", 'mention_adresse_norm_avec_position'] = 'uca_sigle'
    df.loc[df['mention_adresse_norm']  == "uns", 'mention_adresse_norm_avec_position'] = 'uns'
    df.loc[df['mention_adresse_norm'] == "universite_non_mentionnee", 'mention_adresse_norm_avec_position'] = 'universite_non_mentionnee'
    context.log_event(
        AssetObservation(asset_key="normalize_mentionAdresses", metadata={
            "text_metadata": 'Vérif de la normalisation des mentions d\'adresse',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}",
            "result": df[["mention_adresse_norm","mention_adresse_norm_avec_position"]].head(2).to_json()})
    )
    df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/temp_detail_controle_mentionAdresses.csv', index=False, encoding="utf-8")
    return df

@op(required_resource_keys={"config_params"})
def add_structures_aff_labels(context,df):
    df_affs = pd.read_json(f'{context.resources.config_params["primary_data_path"]}/referentiel_structures.json')
    df_affs['affiliation_id'] = df_affs['affiliation_id'].astype('string')
    df['@afids'] = df['@afids'].astype('string')
    df = pd.merge(df, df_affs[['affiliation_id', 'affiliation_name']], how='left', left_on=['@afids'], right_on=['affiliation_id']).drop(columns=['affiliation_id'])
    context.log_event(
        AssetObservation(asset_key="normalize_mentionAdresses", metadata={
            "text_metadata": 'Vérif du merge des afids',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}",
            "result": df[["@afids","mention_adresse_norm","mention_adresse_norm_avec_position"]].head(2).to_json()})
    )
    return df

@op
def regroup_mentions_adresse_by_publis(df):
    return fn.regroup(df,'mention_adresse_norm')

#ajout v2
@op
def regroup_mentions_adresse_position_by_publis(df):
    return fn.regroup(df,'mention_adresse_norm_avec_position')

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
    df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/detail_controle_mentionAdresses.csv', index=False, encoding="utf-8")
    df.to_excel(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/d2p_all_mentions.xlsx')

@op
def clean_regroup_by_publis_data(df):
    # drop columns
    df = df.drop(columns=['@orcid','@auid','ce:indexed-name','mentionAffil_reconstruct', 'mentionAffil_reconstruct_subsentence_cleaned', 'fuzzy_data_uca_dvp', 'fuzzy_score_uca_dvp', 'fuzzy_position_uca_dvp', 'fuzzy_score_uca_sigle', 'fuzzy_score_uns','mention_adresse_norm', 'mention_adresse_norm_avec_position', '@afids', 'affiliation_name'])
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
    # consolide mention_adresse avec position uca
    df_deduplicate.loc[df_deduplicate['regroup_mention_adresse_norm_avec_position'].str.contains('uca_forme_developpee_debut'),'synthese_mention_adresse_norm_avec_position'] = 'uca_forme_developpee_debut'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm_avec_position'].str.contains('uca_forme_developpee_interne')) & (df_deduplicate['synthese_mention_adresse_norm_avec_position'].isna()),'synthese_mention_adresse_norm_avec_position'] = 'uca_forme_developpee_interne'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm_avec_position'].str.contains('uca_sigle')) & (df_deduplicate['synthese_mention_adresse_norm_avec_position'].isna()),'synthese_mention_adresse_norm_avec_position'] = 'uca_sigle'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm_avec_position'].str.contains('uns')) & (df_deduplicate['synthese_mention_adresse_norm_avec_position'].isna()),'synthese_mention_adresse_norm_avec_position'] = 'uns'
    df_deduplicate.loc[(df_deduplicate['regroup_mention_adresse_norm_avec_position'].str.contains('universite_non_mentionnee')) & (df_deduplicate['synthese_mention_adresse_norm_avec_position'].isna()),'synthese_mention_adresse_norm_avec_position'] = 'universite_non_mentionnee'
    return df_deduplicate

@asset(required_resource_keys={"config_params"})
def save_regroup_by_publis_data(context,df):
    context.log_event(
        AssetObservation(asset_key="regroupbypublis_controle_mentionAdresses", metadata={
            "text_metadata": 'Shape of the dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/regroupbypublis_controle_mentionAdresses.csv', index=False, encoding="utf-8")
    df.to_excel(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/d2p_all_publis.xlsx')

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
def consolidate_detail_afids_value_counts(context,df):
    # from detail data
    ## dataframe value_counts par afids -> conversion en dict pour dcc.dropdown [unused finally)]
    df_afids_value_counts = pd.DataFrame(df[["@afids","affiliation_name"]].value_counts()).reset_index().rename(columns={0: "total","@afids": "value"})
    df_afids_value_counts["label"] = df_afids_value_counts["affiliation_name"] + " (" + df_afids_value_counts["total"].astype(str) + ")"
    df_afids_value_counts[["label","value"]].to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/detail_afids_value_counts.csv', index=False, encoding="utf-8")

@asset(required_resource_keys={"config_params"})
def consolidate_regroup_afids_value_counts(context,df):
    ## from detail data
    ## dataframe value_counts of unique dc:identifiers par afids (utilisation de groupby plutôt que crosstab car pas de possibilité de dédoublonner ensuite par dc:identifiers avec crosstab)
    corpus_range = map(str,range(context.resources.config_params["corpus_start_year"],context.resources.config_params["corpus_end_year"]+1,1))
    corpus_range_list = [x for x in corpus_range]
    # group by aff and pub year and count unique dc:identifiers
    df_temp = (df.groupby(['@afids', 'affiliation_name', 'annee_pub']).agg(**{'nb_publis': ('dc:identifiers', 'nunique')})
                             .reset_index()
                             .sort_values(by=['nb_publis'], ascending=[False]))
    # change cols type
    df_temp['annee_pub'] = df_temp['annee_pub'].astype('string')
    df_temp['nb_publis'] = pd.to_numeric(df_temp['nb_publis'], downcast='integer', errors='coerce')
    # pass rows to cols
    df_afids_value_counts = (pd.pivot_table(df_temp,values='nb_publis', index = 'affiliation_name', columns= 'annee_pub', aggfunc= 'sum')
       .reset_index()
       .fillna(0)
       )
    # change cols type
    for column_name in corpus_range_list:
        df_afids_value_counts[column_name] = df_afids_value_counts[column_name].astype('int')
    # add total col
    df_afids_value_counts['nb_publis_total'] = df_afids_value_counts[corpus_range_list].sum(axis=1)
    # rename cols
    df_afids_value_counts=(df_afids_value_counts.rename(columns={ i : f'nb_publis_{i}' for i in corpus_range_list })
                           .sort_values(by=['nb_publis_total'], ascending=[False])
                           )
    # rename value UCA (affiliation) -> labo inconnu
    df_afids_value_counts["affiliation_name"] = df_afids_value_counts["affiliation_name"].str.replace('UCA (affiliation)', 'Labo inconnu', regex=False)
    #save 
    df_afids_value_counts.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_afids_value_counts.csv', index=False, encoding="utf-8")

@asset(required_resource_keys={"config_params"})
def consolidate_top_n_uca_interne(context,df):
    # produit le top 100 des formes les plus utilisées dans les mentions d'adresse
    df = df.loc[df['mention_adresse_norm_avec_position'].isin(['uca_forme_developpee_interne'])]
    df_top_n = (df.groupby(['mentionAffil_reconstruct']).agg(**{'nb_mentions': ('dc:identifiers', 'size')})
               .reset_index()
               .sort_values(by=['nb_mentions'], ascending=[False])
               .nlargest(context.resources.config_params["top_n"], 'nb_mentions'))
    df_top_n.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/detail_top{str(context.resources.config_params["top_n"])}_mentionAffil_reconstruct.csv', index=False, encoding="utf-8")

@asset(required_resource_keys={"config_params"})
def consolidate_all_formes_uca_dvp(context,df):
    df = df.loc[df['mention_adresse_norm'].isin(['uca_forme_developpee'])]
    df["temp"] = (df["mentionAffil_reconstruct"].apply(lambda x: x.split(","))
              .apply(lambda x: list(filter(lambda k: ('Azur' in k) & ("Observatoire" not in k) & ("|" not in k), x))).
               apply(lambda x: x[0].strip() if len(x) ==1 else ''))
    df_uca_formes = pd.DataFrame(df["temp"].value_counts()).reset_index().rename(columns={0: "total","temp": "value"})
    df_uca_formes.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/detail_all_uca_dvp_formes_value_counts.csv', index=False, encoding="utf-8")


@asset(required_resource_keys={"config_params"})
def get_afids_value_counts(context):
    df = pd.read_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/detail_afids_value_counts.csv', sep=",", encoding="utf-8")
    return df

@asset(required_resource_keys={"config_params"})
def consolidate_regroup_crosstabs_adresse_norm(context,df):
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


@asset(required_resource_keys={"config_params"})
def consolidate_regroup_crosstabs_adresse_norm_position(context,df):
    #from regroup data
    ##crosstab annee_pub/synthese_mention_adresse_norm en valeurs absolues avec totaux
    absolute_data = pd.crosstab(df["annee_pub"], df["synthese_mention_adresse_norm_avec_position"],normalize=False, margins=True, margins_name="Total").reset_index()
    absolute_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_avec_position_valeurs_absolues.csv', index=False, encoding="utf-8")
    ##crosstab annee_pub/synthese_mention_adresse_norm en pourcentages avec totaux
    percent_data = (pd.crosstab(df["annee_pub"], df["synthese_mention_adresse_norm_avec_position"],normalize=True, margins=True, margins_name="Total")*100).round(3).reset_index()
    percent_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_avec_position_pourcentages.csv', index=False, encoding="utf-8")
    ##crosstab annee_pub/synthese_mention_adresse_norm en indice base 100 en 2016 et sans totaux
    indice_data = absolute_data.iloc[:-1, :].iloc[:, :-1]
    for c in ['universite_non_mentionnee', 'uca_forme_developpee_interne', 'uca_forme_developpee_debut','uca_sigle', 'uns']:
        indice_data[f'indice_{c}'] = (indice_data[c].div(
            indice_data[c].iloc[0])*100).round(2)
    indice_data.to_csv(f'{context.resources.config_params["reporting_data_path"]}/{context.resources.config_params["observation_date"]}/controle_mention_adresse/consolidation/regroup_crosstab_annee_mention_avec_position_indices.csv', index=False, encoding="utf-8")

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
    #ajouter con.close() partout sinon error database locked

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
    fuzzy_data_uca_dvp = get_fuzzy_data_uca_dvp(nlp_data)
    fuzzy_score_uca_sigle = get_fuzzy_score_uca_sigle(fuzzy_data_uca_dvp)
    fuzzy_data_uns = get_fuzzy_data_uns(fuzzy_score_uca_sigle)
    norm_mention_adresse = normalize_mention_adresse(fuzzy_data_uns)
    norm_mention_adresse_avec_position = normalize_mention_adresse_avec_position(norm_mention_adresse)
    structures_aff_labels = add_structures_aff_labels(norm_mention_adresse_avec_position)
    mentions_adresse_by_publis = regroup_mentions_adresse_by_publis(structures_aff_labels)
    mentions_adresse_position_by_publis = regroup_mentions_adresse_position_by_publis(mentions_adresse_by_publis)
    aff_ids_by_publis = regroup_aff_ids_by_publis( mentions_adresse_position_by_publis)
    aff_names_by_publis = regroup_aff_names_by_publis(aff_ids_by_publis)
    authors_names_by_publis = regroup_authors_names_by_publis(aff_names_by_publis)
    cleaned_regroup_by_publis_data = clean_regroup_by_publis_data(authors_names_by_publis)
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
    consolidate_detail_afids_value_counts(detail_data)
    consolidate_regroup_afids_value_counts(detail_data)
    consolidate_top_n_uca_interne(detail_data)
    consolidate_all_formes_uca_dvp(detail_data)
    consolidate_regroup_crosstabs_adresse_norm(regroup_data)
    consolidate_regroup_crosstabs_adresse_norm_position(regroup_data)

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
def prod_bsi_publis_scopus():
    return [main_fuzzy_process,consolidated_data,save_sqlite]

"""
resources:
  config_params:
    config:
      bert_model: msmarco-distilbert-base-tas-b
      corpus_end_year: 2022
      corpus_start_year: 2016
      db_path: bso_publis_scopus/09_db/publications.db
      feature_data_path: bso_publis_scopus/04_feature
      intermediate_data_path: bso_publis_scopus/02_intermediate
      model_input_data_path: bso_publis_scopus/05_model_input
      model_output_data_path: bso_publis_scopus/07_model_output
      models_path: bso_publis_scopus/06_models
      observation_date: 2022-08-29
      primary_data_path: bso_publis_scopus/03_primary
      raw_data_path: bso_publis_scopus/01_raw
      reporting_data_path: bso_publis_scopus/08_reporting
      top_n: 50
"""


