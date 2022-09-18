import graphlib
from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation, MetadataValue
import pandas as pd
import glob
import os
import subprocess
import lxml

def xsl_process(saxon,xsl,input_file,output_file):
    return subprocess.run(['/bin/bash',saxon,xsl,input_file,output_file])

def get_latest_file(dir,file_extension):
    list_of_files = glob.glob(f'{dir}/*.{file_extension}') 
    latest_file = max(list_of_files, key=os.path.getctime)
    latest_filename = os.path.basename(latest_file)
    return latest_filename

########################## CONFIG ################################

@op(config_schema={"bod_dir_path": str, "bod_xsl_filepath": str, "cyberlibris_dir_path": str, "cyberlibris_xsl_filepath": str, "ftf_dir_path": str, "ftf_xsl_filepath": str,"saxon_path": str})
def get_config(context):
    return context.op_config

########################## BOD ################################

@op
def bod_get_latest_file(context,config_params):
    latest_filename = get_latest_file(f'{config_params["bod_dir_path"]}/01_raw','xml')
    context.log_event(
        AssetObservation(asset_key="last_bod_filename", metadata={
            "text_metadata": "Name of the latest BiblioOnDemand archived file",
            "size": f"{latest_filename}"})
    )
    return latest_filename

@op
def bod_xsl_process(config_params,filename):
    return xsl_process(f'{config_params["saxon_path"]}',f'{config_params["bod_xsl_filepath"]}',f'{config_params["bod_dir_path"]}/01_raw/{filename}',f'{config_params["bod_dir_path"]}/03_primary/result_{filename}')

########################## CYBERLIBRID ################################

@op
def cyberlibris_get_latest_file(context,config_params):
    latest_filename = get_latest_file(f'{config_params["cyberlibris_dir_path"]}/01_raw','xml')
    context.log_event(
        AssetObservation(asset_key="last_cyberlibris_filename", metadata={
            "text_metadata": "Name of the latest cyberlibris archived file",
            "size": f"{latest_filename}"})
    )
    return latest_filename

@op
def cyberlibris_xsl_process(config_params,filename):
    return xsl_process(f'{config_params["saxon_path"]}',f'{config_params["cyberlibris_xsl_filepath"]}',f'{config_params["cyberlibris_dir_path"]}/01_raw/{filename}',f'{config_params["cyberlibris_dir_path"]}/03_primary/result_{filename}')

########################## FTF ################################

atoz_cols_to_remove = ['Edition','Editor', 'Illustrator', 'DOI', 'PeerReviewed','CustomCoverageBegin',
       'CustomCoverageEnd', 'CoverageStatement', 'Embargo', 'CustomEmbargo',
       'Description', 'Subject', 'PackageContentType',
       'CreateCustom', 'HideOnPublicationFinder', 'Delete',
       'OrderedThroughEBSCO', 'IsCustom', 'UserDefinedField1',
       'UserDefinedField2', 'UserDefinedField3', 'UserDefinedField4',
       'UserDefinedField5', 'PackageType', 'AllowEBSCOtoSelectNewTitles','PackageID','VendorName','VendorID']

@op
def ftf_get_latest_file(context,config_params):
    latest_filename = get_latest_file(f'{config_params["ftf_dir_path"]}/01_raw','csv')
    df = pd.read_csv(f'{config_params["ftf_dir_path"]}/01_raw/{latest_filename}',sep=",", encoding="utf-8")
    context.log_event(
        AssetObservation(asset_key="last_ftf_csv_file", metadata={
            "text_metadata": "Shape of the source ftf dataframe",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

@op
def ftf_clean_dataframe(df):
    df = df.drop(atoz_cols_to_remove, axis=1).fillna('').replace('&', '&amp;')
    df.drop(df[(df.PackageName == 'Business Source Complete') & ((df.ResourceType == 'Report') | (df.ResourceType == 'Book Series'))].index, inplace=True)
    return df

@op
def ftf_dummy_filename():
    return "ftf.xml"

@asset
def ftf_save_tmp(context,config_params,df,filename):
    df.to_xml(path_or_buffer=f'{config_params["ftf_dir_path"]}/02_intermediate/temp_{filename}', root_name='Resources', row_name='Resource', encoding='utf-8', xml_declaration=True, pretty_print=True, parser='lxml')
    context.log_event(
        AssetObservation(asset_key="intermediate_xml_file", metadata={
            "text_metadata": "Shape of the intermediate dataframe converted in xml format",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    return df

@op
def ftf_xsl_process(config_params,df,filename):
    return xsl_process(f'{config_params["saxon_path"]}',f'{config_params["ftf_xsl_filepath"]}',f'{config_params["ftf_dir_path"]}/02_intermediate/temp_{filename}',f'{config_params["ftf_dir_path"]}/03_primary/result_{filename}')


########################## JOBS ################################
################################################################

@job(name="bod_flow",
     metadata={
        "url_bod_admin": MetadataValue.url("https://univ-cotedazur.biblioondemand.com/"),
        "login_bod_admin": MetadataValue.text("login:ggeoffroy/mdp:ggeoffroy"),
        "workflow": MetadataValue.text("1. Export BOD admin : accès professionnel -> Mes acquisitions -> Titres acquis -> Export ISO-2709 ; 2. Conversion Unimarc ISO 2709 séquentiel .mrc en Unimarc xml avec le module MarcTools de MarcEditor ; 3. dagster flow ; 4. UTF-8 (sans BOM) -> Double-zipper en .tar.gz ->  /exlibris/aleph/aleph_export_2_primo/Numilog_export_2_primo (supprimer le fichier précédent) -> Pipe Primo Numilog_Delete_Reload (modifier la date)")
    }
)
def bod_flow():
    config_params = get_config()
    bod_xsl_process(config_params,bod_get_latest_file(config_params))

@job(name="cyberlibris_flow",
    metadata={
        "url_cyberlibris_admin": MetadataValue.url("https://extranet2.cyberlibris.com/index/login"),
        "login_bod_admin": MetadataValue.text("login:admin-uniaz/mdp:cyberlibris"),
        "workflow": MetadataValue.text("1. Export Cyberlibris admin : Onglet Notices -> Complètes -> ISO -> Exports de Scholarvox_universite_emploi__metiers_et_formation_DATE_LA_PLUS_RECENTE.pan et Scholarvox_universite_sciences_eco_gestion_DATE_LA_PLUS_RECENTE.pan; 2. Conversion Unimarc ISO 2709 séquentiel .mrc en Unimarc xml avec le module MarcTools de MarcEditor ; 3. dagster flow ; 4. UTF-8 (sans BOM) -> Double-zipper en .tar.gz -> /exlibris/aleph/aleph_export_2_primo/Cyberlibris -> Pipe Primo Cyberlibris_Delete_Reload")
    }
)
def cyberlibris_flow():
    config_params = get_config()
    cyberlibris_xsl_process(config_params,cyberlibris_get_latest_file(config_params))

@job(name="ftf_flow",
    metadata={
        "url_ftf_admin": MetadataValue.url("https://eadmin.ebscohost.com/eadmin/Login.aspx"),
        "login_ftf_admin": MetadataValue.text("login:S4106903/mdp:Docelec@2021"),
        "workflow": MetadataValue.text("1. Export FTF admin : holdings management -> Download ; 2. dagster flow ; 4. UTF-8 (sans BOM) -> Double-zipper en .tar.gz ->  /exlibris/aleph/AtoZ_export_2_primo -> Pipe Primo ATOZ_Delete_Reload")
    }
)
def ftf_flow():
    config_params = get_config()
    df = ftf_clean_dataframe(ftf_get_latest_file(config_params))
    filename = ftf_dummy_filename()
    save_tmp = ftf_save_tmp(config_params,df,filename)
    ftf_xsl_process(config_params,save_tmp,filename)

@repository
def docelec_signalement():
    return [bod_flow,cyberlibris_flow,ftf_flow]

"""
Config
ops:
  get_config:
    config:
      saxon_path: docelec_signalement/run_saxon.sh
      bod_dir_path: docelec_signalement/biblioondemand
      bod_xsl_filepath: docelec_signalement/biblioondemand/biblioondemand4primo.xsl
      cyberlibris_dir_path: docelec_signalement/cyberlibris
      cyberlibris_xsl_filepath: docelec_signalement/cyberlibris/cyberlibris4primo.xsl 
      ftf_dir_path: docelec_signalement/ftf
      ftf_xsl_filepath : docelec_signalement/ftf/ftf4primo.xsl
"""