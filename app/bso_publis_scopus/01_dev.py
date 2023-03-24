import graphlib
from dagster import job, op, graph, repository, asset, AssetIn, AssetMaterialization, AssetObservation, Out, In, String, MetadataValue, make_values_resource
import pandas as pd
import numpy as np
import pymysql
import sqlalchemy as sqla

########################## [OLD] 01_MAIN_FUZZY ################################

@op(required_resource_keys={"config_params"})
def db_connexion(context):
    user = context.resources.config_params["user"]
    password = context.resources.config_params["password"]
    host = context.resources.config_params["host"]
    database = context.resources.config_params["database"]
    engine = sqla.create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
    query = 'SELECT * FROM referentiel_structures LIMIT 5'
    df = pd.read_sql_query(sql=sqla.text(query), con=engine.connect())
    print(df)
    return df.to_json(orient='records')

@op
def test_query(cursor):
    query = 'SELECT * FROM referentiel_structures LIMIT 5'
    cursor.execute(query)
    return cursor.fetchall()


@job(name="01_test_mysql",
     resource_defs={"config_params": make_values_resource()},
    metadata={
        "notes": MetadataValue.text("1. ")
    }
)
def dev_mysql():
    test_query(db_connexion())

@repository
def dev_bso_publis_scopus():
    return [dev_mysql]


"""
resources:
  config_params:
    config:
      user: gazw6343_ggeoffroy
      password: CLg#6z[_tUyW
      host: nout.o2switch.net
      database: gazw6343_si_publications
"""


