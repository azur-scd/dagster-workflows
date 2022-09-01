import graphlib
from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation
import glob
import os
import subprocess

@op(config_schema={"raw_data_path": str})
def load_last_cyberlibris_xml_file(context):
    list_of_files = glob.glob(f'{context.op_config["raw_data_path"]}/*.xml') 
    latest_file = max(list_of_files, key=os.path.getctime)
    latest_filename = os.path.basename(latest_file)
    context.log_event(
        AssetObservation(asset_key="last_cyberlibris_filename", metadata={
            "text_metadata": "Name of the latest cyberlibris archived file",
            "size": f"nb lignes {latest_filename}"})
    )
    context.log.info(latest_filename)
    return latest_filename

@op(config_schema={"raw_data_path": str, "primary_data_path": str, "saxon_path": str, "xsl_path": str})
def xsl_process(context, filename):
    return subprocess.run(['/bin/bash',f'{context.op_config["saxon_path"]}/run_saxon.sh',f'{context.op_config["xsl_path"]}/cyberlibris4primo.xsl',f'{context.op_config["raw_data_path"]}/{filename}',f'{context.op_config["primary_data_path"]}/result_{filename}'])

@job
def run_workflow():
    xsl_process(load_last_cyberlibris_xml_file())

@repository
def prod_cyberlibris_workflow():
    return [run_workflow]