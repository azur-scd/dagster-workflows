import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import glob
import helpers.functions as fn
import sqlite3

########################## [OLD] FUNCTIONS ################################

@op(required_resource_keys={"config_params"})
def load_bert_model(context):
    model = SentenceTransformer(f'{context.resources.config_params["bert_model"]}')
    return model

def to_bso_class_with_ml(row,ml_model):
    bso_classes = {0: 'Biology (fond.)', 1: 'Chemistry', 2: 'Computer and \n information sciences', 3: 'Earth, Ecology, \nEnergy and applied biology', 4: 'Engineering',
	               5: 'Humanities', 6: 'Mathematics', 7: 'Medical research', 8: 'Physical sciences, Astronomy', 9: 'Social sciences'} 
    predicted_class = ml_model.predict([[row]])
    return bso_classes[predicted_class[0]]

# Fuzzy extractor v1
def fuzzy_extractone_uca_developpee(l):
    return max(process.extractOne("universite cote azur",l)[1],process.extractOne("university cote azur",l)[1], process.extractOne("azur university",l)[1],process.extractOne("univ cote azur",l)[1],process.extractOne("universite cote 'azur",l)[1],process.extractOne("university cote 'azur",l)[1],process.extractOne("univ cote 'azur",l)[1])
def fuzzy_extractone_uca_sigle(l):
    return process.extractOne("uca",  l)[1]
def fuzzy_extractone_uns_developpee(l):
    return max(process.extractOne("universite nice",l)[1],process.extractOne("universite nice sophia",l)[1],process.extractOne("university nice",l)[1],process.extractOne("university nice sophia",l)[1],process.extractOne("antipolis university",l)[1],process.extractOne("univ nice",l)[1])
def fuzzy_extractone_uns_sigle(l):
    return process.extractOne("uns",l)[1]

# Fuzzy extractor v2
def fuzzy_uca_developpee(l):
    l = [s.replace("'","") for s in l]
    return max(process.extractOne("universite cote azur",l)[1],
               process.extractOne("university cote azur",l)[1], 
               process.extractOne("universite azur",l)[1],
               process.extractOne("cote azur university",l)[1], 
               process.extractOne("univ cote azur",l)[1],
               process.extractOne("universitcrossed sign cote azur",l)[1],
               process.extractOne("universitcrossed cote azur",l)[1],
               process.extractOne("universit x00e9 c x00f4 te x2019 azur",l)[1])
def fuzzy_uca_sigle(l):
    return max(process.extractOne("uca",  l)[1],fuzz.partial_ratio("uca",l)) 
def fuzzy_uns(l):
    return max(process.extractOne("universite nice",l)[1],
               process.extractOne("universite nice sophia",l)[1],
               process.extractOne("nice sophia antipolis",l)[1],
               process.extractOne("universite sophia antipolis",l)[1],
               process.extractOne("universite nice sophia antipolis",l)[1],
               process.extractOne("university nice",l)[1],
               process.extractOne("u nice",l)[1],
               process.extractOne("university nice sophia",l)[1],
               process.extractOne("university sophia antipolis",l)[1],
               process.extractOne("sophia antipolis university",l)[1],
               process.extractOne("antipolis university",l)[1],
               process.extractOne("nice university",l)[1],
               process.extractOne("univ nice",l)[1],
               process.extractOne(" uns",l)[1],
               process.extractOne("uns",l)[1])
def fuzzy_uns_sigle(l):
    return max(process.extractOne("uns",  l)[1],fuzz.partial_ratio("uns",l))

# Bert & fuzzy functions

def apply_model(model,univ_forme,affil_forme):
    cos_sin_list = list()
    for i in affil_forme:
        # util.cos_sim(emb2, model.encode(str(i))) est un objet de type pytorch.tensor
        cos_sin_list.append(util.cos_sim(model.encode(univ_forme), model.encode((str(i)))).item())
    return cos_sin_list

def process_bert_data_uca_dvp(model,x):
  # returns a tuple (max_value, position)
  #line = get_ngrams(x,3)
  result = apply_model(model,"universite cote azur",x)
  max_value = max(result)
  return (max_value, result.index(max_value)+1)

def process_bert_data_uns_dvp(model,x):
  result = apply_model(model,"universite nice sophia antipolis",x)
  max_value = max(result)
  return max_value


########################## [OLD] 01_MAIN_FUZZY ################################

@op
def transform_fuzzy_data_v1(df):
    df["fuzzy_extractone_uca_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uca_developpee)
    df["fuzzy_extractone_uca_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uca_sigle)
    df["fuzzy_extractone_uns_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uns_developpee)
    df["fuzzy_extractone_uns_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_extractone_uns_sigle)
    return df

@op
def transform_fuzzy_data_v2(df):
    df["fuzzy_uca_developpee"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_uca_developpee)
    df["fuzzy_uca_sigle"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_uca_sigle)
    df["fuzzy_uns"] = df["mentionAffil_reconstruct_subsentence_cleaned"].apply(fn.fuzzy_uns)
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

@asset(required_resource_keys={"config_params"})
def process_classification(context,mesri_bso_dataset,publis_uniques_doi_oa_data,logmodel, bso_classes):
    #1ère étape : on duplique le dataset oa
    temp = publis_uniques_doi_oa_data[["doi","title","journal_name","publisher"]]
    #2ème étape : on ajoute une col 
    temp["bso_classification"] = np.nan
    #3ème étape : on récupère la bso_classification du mesri pour les doi présents
    col = 'doi'
    cols_to_replace = ['bso_classification']
    temp.loc[temp[col].isin(mesri_bso_dataset[col]), cols_to_replace] = mesri_bso_dataset.loc[mesri_bso_dataset[col].isin(temp[col]),cols_to_replace].values
    #4ème étape : préparation NLP sur les publis non classifiées restantes
    temp1 = temp[(temp.bso_classification.isna()) & (temp.title.notna()) & (temp.publisher.notna()) & (temp.journal_name.notna())]
    temp1["feature"] = temp1["title"] + ' '  +  temp1["journal_name"] + ' '  +  temp1["publisher"]
    temp1['cleaned_feature'] = (
            temp1['feature']
            .pipe(hero.clean)
            .apply(fn.remove_stopwords_fr)
            .apply(fn.remove_special_characters)
            .apply(fn.lemmatize)
    )
    #5ème étape : on applique le modèle et on complète le dataste temp
    #temp1["bso_classification"] = temp1.apply(lambda row: fn.to_bso_class_with_ml(row["cleaned_feature"],logmodel),axis=1)
    context.log_event(
        AssetObservation(asset_key="temp1", metadata={
            "text_metadata": 'Vérification intermédiaire',
            "result": temp1["cleaned_feature"].head(2).to_json()})
    )
    temp1["bso_classification"] = logmodel.predict(temp1)
    temp1["bso_classification"] = temp1["bso_classification"].apply(lambda x: bso_classes[x])
    temp.loc[temp[col].isin(temp1[col]), cols_to_replace] = temp1.loc[temp1[col].isin(temp[col]),cols_to_replace].values
    context.log_event(
        AssetObservation(asset_key="subset_of_all_classification_operations", metadata={
            "text_metadata": 'Subset résultat de toutes les opérations de classification',
            "size": f'nb lignes {temp.shape[0]}, nb cols {temp.shape[1]}'})
    )
    temp.to_csv(f'{context.resources.config_params["model_output_data_path"]}/{context.resources.config_params["observation_date"]}/uca_doi_classified.csv',index = False,encoding='utf8')
    return temp


@asset(required_resource_keys={"config_params"})
def complete_classification_labels(context,temp):
    df_classification_mapping = pd.read_json(f'{context.resources.config_params["primary_data_path"]}/bso_classification_mapping.json')
    temp_mapped=pd.merge(temp,df_classification_mapping, left_on='bso_classification', right_on='name_en',how="left").drop(columns=['name_en']).rename(columns={"name_fr": "bso_classification_fr"})
    temp_mapped.loc[temp_mapped['bso_classification'].isin(['Earth, Ecology, \\nEnergy and applied biology']), 'bso_classification_fr'] = 'Sciences de la Terre, écologie, énergie et biologie appliquée'
    temp_mapped.loc[temp_mapped['bso_classification'].isin(['Computer and information sciences']), 'bso_classification_fr'] = 'Informatique et sciences de l\'information'
    temp_mapped.loc[temp_mapped['bso_classification'].isin(['Earth, Ecology, \\nEnergy and applied biology']), 'main_domain'] = 'Sciences, Technologies, Santé'
    temp_mapped.loc[temp_mapped['bso_classification'].isin(['Computer and information sciences']), 'bso_classification_fr'] = 'Sciences, Technologies, Santé'
    return temp_mapped
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


