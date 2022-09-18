import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, Nothing
import pandas as pd
import numpy as np
import glob
import os
import subprocess

@op(config_schema={"primary_data_path": str, "observation_date": str})
def create_data_observation_subfolders(context):
    concerned_folders = ["primary_data_path"]
    for i in concerned_folders:
        if not os.path.exists(f'{context.op_config[i]}/{context.op_config["observation_date"]}'):
            os.mkdir(f'{context.op_config[i]}/{context.op_config["observation_date"]}')

@op(config_schema={"raw_data_path": str, "intermediate_data_path": str, "observation_date": str, "bibliotheque": str})
def extract_data_source(context):
    df = pd.read_excel(f'{context.op_config["raw_data_path"]}/{context.op_config["observation_date"]}/{context.op_config["bibliotheque"]}.xlsx')
    context.log_event(
        AssetObservation(asset_key="initial_jp_dataset", metadata={
            "text_metadata": 'Shape of the initial JP dataset',
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    df[['PPN']].to_xml(f'{context.op_config["intermediate_data_path"]}/{context.op_config["bibliotheque"]}.xml', index=False, encoding="utf-8")
    return df


@op(config_schema={"intermediate_data_path": str, "saxon_path": str, "xsl_path": str, "observation_date": str, "bibliotheque": str})
def xsl_process(context,df):
    subprocess.run(['/bin/bash',f'{context.op_config["saxon_path"]}/run_saxon.sh',f'{context.op_config["xsl_path"]}/aleph_ws_ppn2docnumber.xsl',f'{context.op_config["intermediate_data_path"]}/{context.op_config["bibliotheque"]}.xml',f'{context.op_config["intermediate_data_path"]}/result_{context.op_config["bibliotheque"]}.xml'])
    return df


@asset(config_schema={"intermediate_data_path": str, "bibliotheque": str})
def monitore_not_missing_docnumber(context,d):
    df = pd.read_xml(f'{context.op_config["intermediate_data_path"]}/result_{context.op_config["bibliotheque"]}.xml', xpath=".//row").assign(zip = lambda x: x["doc_number"].str.replace('.0', ''))
    df_missing = df[df.set_number.isna()][['ppn']]
    df_missing.to_xml(f'{context.op_config["intermediate_data_path"]}/missing_ppn_{context.op_config["bibliotheque"]}.xml', index=False, encoding="utf-8")
    context.log_event(
        AssetObservation(asset_key="df_missing_doc_number", metadata={
            "text_metadata": 'Pourcentage de ppn absenst d\'Aleph',
            "size": f"{df_missing.shape[0] / df.shape[0] * 100} %"})
    )
    return df[df.set_number.notna()]


@op(config_schema={"intermediate_data_path": str, "primary_data_path": str, "observation_date": str, "bibliotheque": str})
def transform_data(context,df_not_missing_docnumber):
    with open(f'{context.op_config["primary_data_path"]}/{context.op_config["observation_date"]}/{context.op_config["bibliotheque"]}.seq', "w") as f:
        for index, row in df_not_missing_docnumber.iterrows():
            ppn = str(row["ppn"]).zfill(9)
            doc_number = str(row["doc_number"][3:]).zfill(9)
            #f.write(f'{str(row["doc_number"]).zfill(9)} CUL   L $$a{context.op_config["bibliotheque"]}$$b_loc_$$c_typecote_$$i_cote_$$fHDL-{ppn}$$e_numinv_$$gVoir la notice Sudoc https://www.sudoc.fr/{ppn}$$k_noteinterne_$$r_noteopac_$$s_notecirc_$$m_statutex_$$n_statuttraitement_$$o_991_$$l_numcommande_$$dCOLLE\r')
            f.write(f'{doc_number} CUL   L $$a{context.op_config["bibliotheque"]}$$b$$c$$i$$fHDL-{ppn}$$e$$gVoir notice Sudoc https://www.sudoc.fr/{ppn}$$k$$r$$s$$m20$$nCO$$o$$l$$dCOLLE\r')

@job(
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


