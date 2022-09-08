import graphlib
from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation, MetadataValue
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

@job(
    metadata={
        "url_cyberlibris_admin": MetadataValue.url("https://extranet2.cyberlibris.com/index/login"),
        "login_bod_admin": MetadataValue.text("login:admin-uniaz/mdp:cyberlibris"),
        "workflow": MetadataValue.text("1. Export Cyberlibris admin : Onglet Notices -> Complètes -> ISO -> Exports de Scholarvox_universite_emploi__metiers_et_formation_DATE_LA_PLUS_RECENTE.pan et Scholarvox_universite_sciences_eco_gestion_DATE_LA_PLUS_RECENTE.pan; 2. Conversion Unimarc ISO 2709 séquentiel .mrc en Unimarc xml avec le module MarcTools de MarcEditor ; 3. dagster flow ; 4. UTF-8 (sans BOM) -> Double-zipper en .tar.gz -> /exlibris/aleph/aleph_export_2_primo/Cyberlibris -> Pipe Primo Cyberlibris_Delete_Reload")
    }
)
def cyberlibris_flow():
    xsl_process(load_last_cyberlibris_xml_file())

@repository
def docelec_signalement():
    return [cyberlibris_flow]

"""
Config
ops:
  load_last_cyberlibris_xml_file:
    config:
      raw_data_path: "docelec_signalement/cyberlibris/01_raw"
  xsl_process:
    config:
      saxon_path: "docelec_signalement"
      xsl_path: "docelec_signalement/cyberlibris"
      raw_data_path: "docelec_signalement/cyberlibris/01_raw"
      primary_data_path: "docelec_signalement/cyberlibris/03_primary"
"""