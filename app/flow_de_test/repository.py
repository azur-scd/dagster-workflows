from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation
import pandas as pd 
import os
import functions as fn

@op(config_schema={"path": str,"uca_etabs": list})
def get_config(context):
    return context.op_config

@op
def upstream_asset(config_params): 
    return fn.upstream_asset(config_params)


@asset
def define_dataframe():
    return fn.define_dataframe()

@asset
def drop_column(context,df):
    df1 = fn.drop_column(df)
    context.log_event(
        AssetObservation(asset_key="returned_df", metadata={
            "text_metadata": "Shape of the dataframe after transformation",
            "size": f"nb lignes {df.shape[0]}, nb cols {df.shape[1]}"})
    )
    context.log_event(
        AssetMaterialization(
            asset_key="returned_df", description="Persisted result to storage"
        )
    )
    return df1

@op
def save_local_file(config_params,df,uca_etabs):
    for i in uca_etabs:
        df.to_csv(f'{config_params["path"]}/{i}/test.csv',index=False)


@job
def hello_world_job_01():
    config_params = get_config()
    final_df =  drop_column(define_dataframe())
    save_local_file(config_params,final_df,upstream_asset(config_params))


@repository
def prod_repo():
    return [hello_world_job_01]

@repository
def dev_repo():
    return [hello_world_job_01]