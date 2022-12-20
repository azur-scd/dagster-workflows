import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import glob
import os
import subprocess

@op(required_resource_keys={"config_params"})
def create_data_observation_subfolders(context):
    concerned_folders = ["primary_data_path"]
    for i in concerned_folders:
        if not os.path.exists(f'{context.resources.config_params[i]}/{context.resources.config_params["observation_date"]}'):
            os.mkdir(f'{context.resources.config_params[i]}/{context.resources.config_params["observation_date"]}')

@op(required_resource_keys={"config_params"})
def extract_data_source(context):
    df = pd.read_excel(f'{context.resources.config_params["raw_data_path"]}/{context.resources.config_params["observation_date"]}/{context.resources.config_params["bibliotheque"]}.xlsx')
    context.log_event(
        AssetObservation(asset_key="initial_dataset", metadata={
            "text_metadata": 'Shape of the initial dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df[['PPN']].to_xml(f'{context.resources.config_params["intermediate_data_path"]}/{context.resources.config_params["bibliotheque"]}.xml', index=False, encoding="utf-8")
    return df


@op(required_resource_keys={"config_params"})
def xsl_process(context,df):
    subprocess.run(['/bin/bash',f'{context.resources.config_params["saxon_path"]}/run_saxon.sh',f'{context.resources.config_params["xsl_path"]}/aleph_ws_ppn2docnumber.xsl',f'{context.resources.config_params["intermediate_data_path"]}/{context.resources.config_params["bibliotheque"]}.xml',f'{context.resources.config_params["intermediate_data_path"]}/result_{context.resources.config_params["bibliotheque"]}.xml'])
    return df

@asset(required_resource_keys={"config_params"})
def monitore_not_missing_docnumber(context,d):
    df = pd.read_xml(f'{context.resources.config_params["intermediate_data_path"]}/result_{context.resources.config_params["bibliotheque"]}.xml', xpath=".//row").assign(zip = lambda x: x["doc_number"].str.replace('.0', ''))
    df_missing = df[df.set_number.isna()][['ppn']]
    df_missing.to_xml(f'{context.resources.config_params["intermediate_data_path"]}/missing_ppn_{context.resources.config_params["bibliotheque"]}.xml', index=False, encoding="utf-8")
    context.log_event(
        AssetObservation(asset_key="df_missing_doc_number", metadata={
            "text_metadata": 'Pourcentage de ppn absenst d\'Aleph',
            "size": f"{df_missing.shape[0] / df.shape[0] * 100} %"})
    )
    return df[df.set_number.notna()]


@op(required_resource_keys={"config_params"})
def transform_data(context,df_not_missing_docnumber):
    with open(f'{context.resources.config_params["primary_data_path"]}/{context.resources.config_params["observation_date"]}/{context.resources.config_params["bibliotheque"]}.seq', "w") as f:
        for index, row in df_not_missing_docnumber.iterrows():
            ppn = str(row["ppn"]).zfill(9)
            doc_number = str(row["doc_number"][3:]).zfill(9)
            #f.write(f'{str(row["doc_number"]).zfill(9)} CUL   L $$a{context.resources.config_params["bibliotheque"]}$$b_loc_$$c_typecote_$$i_cote_$$fHDL-{ppn}$$e_numinv_$$gVoir la notice Sudoc https://www.sudoc.fr/{ppn}$$k_noteinterne_$$r_noteopac_$$s_notecirc_$$m_statutex_$$n_statuttraitement_$$o_991_$$l_numcommande_$$dCOLLE\r')
            f.write(f'{doc_number} CUL   L $$a{context.resources.config_params["bibliotheque"]}$$b$$c$$i$$fHDL-{ppn}$$e$$gVoir notice Sudoc https://www.sudoc.fr/{ppn}$$k$$r$$s$$m20$$nCO$$o$$l$$dCOLLE\r')

@job(name="01_examplaires_seuls",
    resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("Exemple de chaînage d'ops sans output")
    }
)
def periodiques_flow():
    #configs
    create_data_observation_subfolders()
    data_source = extract_data_source()
    process = xsl_process(data_source)
    not_missing_docnumber = monitore_not_missing_docnumber(process)
    transform_data(not_missing_docnumber)


@repository
def dev_aleph_uniceloader():
    return [periodiques_flow]

"""
Config
ops:
  create_data_observation_subfolders:
    config:
      observation_date: 2022-09-16
      primary_data_path: aleph_uniceloader/03_primary
  extract_data_source:
    config:
      observation_date: 2022-09-16
      bibliotheque: OCANI
      raw_data_path: aleph_uniceloader/01_raw
      intermediate_data_path: aleph_uniceloader/02_intermediate
  xsl_process:
    config:
      observation_date: 2022-09-16
      bibliotheque: OCANI
      saxon_path: aleph_uniceloader
      xsl_path: aleph_uniceloader
      intermediate_data_path: aleph_uniceloader/02_intermediate
  monitore_not_missing_docnumber:
    config:
      bibliotheque: OCANI 
      intermediate_data_path: aleph_uniceloader/02_intermediate
  transform_data:
    config:
      observation_date: 2022-09-16
      bibliotheque: OCANI
      primary_data_path: aleph_uniceloader/03_primary
      intermediate_data_path: aleph_uniceloader/02_intermediate     
"""

"""
Config
resources:
  config_params:
    config:
      observation_date: 2022-09-16
      bibliotheque: OCANI
      raw_data_path: aleph_uniceloader/01_raw
      intermediate_data_path: aleph_uniceloader/02_intermediate
      primary_data_path: aleph_uniceloader/03_primary
      saxon_path: aleph_uniceloader
      xsl_path: aleph_uniceloader
"""


