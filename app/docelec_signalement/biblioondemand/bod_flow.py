import graphlib
from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation, MetadataValue
import glob
import os
import subprocess

@op(config_schema={"raw_data_path": str})
def load_last_bod_xml_file(context):
    list_of_files = glob.glob(f'{context.op_config["raw_data_path"]}/*.xml') 
    latest_file = max(list_of_files, key=os.path.getctime)
    latest_filename = os.path.basename(latest_file)
    context.log_event(
        AssetObservation(asset_key="last_bod_filename", metadata={
            "text_metadata": "Name of the latest cyberlibris archived file",
            "size": f"{latest_filename}"})
    )
    context.log.info(latest_filename)
    return latest_filename

@op(config_schema={"raw_data_path": str, "primary_data_path": str, "saxon_path": str, "xsl_path": str})
def xsl_process(context, filename):
    return subprocess.run(['/bin/bash',f'{context.op_config["saxon_path"]}/run_saxon.sh',f'{context.op_config["xsl_path"]}/biblioondemand4primo.xsl',f'{context.op_config["raw_data_path"]}/{filename}',f'{context.op_config["primary_data_path"]}/result_{filename}'])

@job(
    metadata={
        "url_bod_admin": MetadataValue.url("https://univ-cotedazur.biblioondemand.com/"),
        "login_bod_admin": MetadataValue.text("login:ggeoffroy/mdp:ggeoffroy"),
        "workflow": MetadataValue.text("1. Export BOD admin : accès professionnel -> Mes acquisitions -> Titres acquis -> Export ISO-2709 ; 2. Conversion Unimarc ISO 2709 séquentiel .mrc en Unimarc xml avec le module MarcTools de MarcEditor ; 3. dagster flow ; 4. UTF-8 (sans BOM) -> Double-zipper en .tar.gz ->  /exlibris/aleph/aleph_export_2_primo/Numilog_export_2_primo (supprimer le fichier précédent) -> Pipe Primo Numilog_Delete_Reload (modifier la date)")
    }
)
def bod_flow():
    xsl_process(load_last_bod_xml_file())

@repository
def docelec_signalement():
    return [bod_flow]

"""
Config
ops:
  load_last_bod_xml_file:
    config:
      raw_data_path: "docelec_signalement/biblioondemand/01_raw"
  xsl_process:
    config:
      saxon_path: "docelec_signalement"
      xsl_path: "docelec_signalement/biblioondemand"
      raw_data_path: "docelec_signalement/biblioondemand/01_raw"
      primary_data_path: "docelec_signalement/biblioondemand/03_primary"
"""