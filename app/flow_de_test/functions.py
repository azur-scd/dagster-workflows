from dagster import job, op, repository, asset, AssetIn, AssetMaterialization, AssetObservation
import pandas as pd 
import os


def upstream_asset(config_params):
    uca_etabs = config_params["uca_etabs"]
    for i in uca_etabs:
        if not os.path.exists(f'{config_params["path"]}/{i}'):
            os.mkdir(f'{config_params["path"]}/{i}')   
    return uca_etabs


def define_dataframe():
    df = pd.DataFrame({'name': ['Raphael', 'Donatello'],
                   'mask': ['red', 'purple'],
                   'weapon': ['sai', 'bo staff']})
    return df


def drop_column(df):
    df1 = df.drop(['weapon'], axis=1)
    return df1
