import graphlib
from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation
import pandas as pd
import glob
import os
import subprocess
import lxml

atoz_cols_to_remove = ['Edition','Editor', 'Illustrator', 'DOI', 'PeerReviewed','CustomCoverageBegin',
       'CustomCoverageEnd', 'CoverageStatement', 'Embargo', 'CustomEmbargo',
       'Description', 'Subject', 'PackageContentType',
       'CreateCustom', 'HideOnPublicationFinder', 'Delete',
       'OrderedThroughEBSCO', 'IsCustom', 'UserDefinedField1',
       'UserDefinedField2', 'UserDefinedField3', 'UserDefinedField4',
       'UserDefinedField5', 'PackageType', 'AllowEBSCOtoSelectNewTitles','PackageID','VendorName','VendorID']


@asset(config_schema={"raw_data_path": str})
def load_last_ftf_csv_file(context):
    list_of_files = glob.glob(f'{context.op_config["raw_data_path"]}/*.csv') 
    latest_file = max(list_of_files, key=os.path.getctime)
    latest_filename = os.path.basename(latest_file)
    df = pd.read_csv(f'{context.op_config["raw_data_path"]}/{latest_filename}',sep=",", encoding="utf-8")
    context.log_event(
        AssetObservation(asset_key="last_ftf_csv_file", metadata={
            "text_metadata": "Shape of the source ftf dataframe",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

@op
def clean_dataframe(df):
    df = df.drop(atoz_cols_to_remove, axis=1).fillna('').replace('&', '&amp;')
    df.drop(df[(df.PackageName == 'Business Source Complete') & ((df.ResourceType == 'Report') | (df.ResourceType == 'Book Series'))].index, inplace=True)
    return df

@asset(config_schema={"intermediate_data_path": str})
def save_tmp(context,df):
    df.to_xml(path_or_buffer=f'{context.op_config["intermediate_data_path"]}/ftf_temp.xml', root_name='Resources', row_name='Resource', encoding='utf-8', xml_declaration=True, pretty_print=True, parser='lxml')
    context.log_event(
        AssetObservation(asset_key="intermediate_xml_file", metadata={
            "text_metadata": "Shape of the intermediate dataframe converted in xml format",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

@op(config_schema={"intermediate_data_path": str, "primary_data_path": str, "saxon_path": str, "xsl_path": str})
def xsl_process(context):
    return subprocess.run(['/bin/bash',f'{context.op_config["saxon_path"]}/run_saxon.sh',f'{context.op_config["xsl_path"]}/ftf4primo.xsl',f'{context.op_config["intermediate_data_path"]}/ftf_temp.xml',f'{context.op_config["primary_data_path"]}/ftf.xml'])

@job
def run_workflow():
    df = clean_dataframe(load_last_ftf_csv_file())
    save_tmp(df)
    xsl_process()

@repository
def prod_ftf_workflow():
    return [run_workflow]