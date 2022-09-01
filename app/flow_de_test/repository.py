from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation
import pandas as pd 
import os

@op(config_schema={"path": str,"uca_etabs": list})
def get_config(context):
    return context.op_config


@op(config_schema={"path": str,"uca_etabs": list})
def upstream_asset(context):
    uca_etabs = context.op_config["uca_etabs"]
    for i in uca_etabs:
        if not os.path.exists(f'{context.op_config["path"]}/{i}'):
            os.mkdir(f'{context.op_config["path"]}/{i}')   
    return uca_etabs


@asset
def define_dataframe():
    df = pd.DataFrame({'name': ['Raphael', 'Donatello'],
                   'mask': ['red', 'purple'],
                   'weapon': ['sai', 'bo staff']})
    return df

@asset(ins={"df": AssetIn(key="define_dataframe")})
def drop_column(context,df):
    df1 = df.drop(['weapon'], axis=1)
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

@op(config_schema={"path": str})
def save_local_file(context,df,uca_etabs):
    for i in uca_etabs:
        df.to_csv(f'{context.op_config["path"]}/{i}/test.csv',index=False)



@job
def hello_world_job():
    final_df = drop_column(define_dataframe())
    save_local_file(final_df,upstream_asset())


@repository
def prod_repo():
    return [hello_world_job]

@repository
def dev_repo():
    return [hello_world_job]