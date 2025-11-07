import awswrangler as wr
import pandas as pd

import sqlalchemy


from utils.logger import get_logger

logger = get_logger(__name__)

def postgres_connect(secret_id):
  try:
    conn = wr.postgresql.connect(secret_id=secret_id)
    logger.debug(f"Success to connect on Postgres secret {secret_id[:((len(secret_id))//2)]}***")
    return conn
  except Exception as e:
    logger.error(f"Fail to connect on postgres database with secret {secret_id[:((len(secret_id))//2)]}***")
    logger.error(e)
    raise e

def postgres_execute_query(secret_id, query):
  try:
    with postgres_connect(secret_id=secret_id) as pg_connection:
      df = wr.postgresql.read_sql_query(
        sql = query,
        con = pg_connection
      )
      logger.debug(f"Success query on athena: \n{query}")
    return df
  except Exception as e:
    logger.error("Fail to fetch data from Postgres")
    logger.error(e)

def postgres_to_sql_from_secret(secret_id, df:pd.DataFrame, table, schema, mode='append', **kwargs):
  try:
    with postgres_connect(secret_id=secret_id) as pg_connection:
      logger.debug(f"Save {df.shape[0]} lines on {schema}.{table} with {mode} mode")
      postgres_to_sql_from_connection(pg_connection, df ,table ,schema ,mode ,**kwargs)
  except Exception as e:
    logger.error("Fail send data to Postgres")
    logger.error(e)

def postgres_to_sql_from_connection(pg_connection, df:pd.DataFrame, table, schema, mode='append', **kwargs):
  try:
    logger.debug(f"Save {df.shape[0]} lines on {schema}.{table} with {mode} mode")
    wr.postgresql.to_sql(
      con               = pg_connection, 
      df                = df, 
      table             = table, 
      schema            = schema,
      mode              = mode,
      index             = False,
      use_column_names  = True,
      **kwargs
    )
  except Exception as e:
    logger.error("Fail send data to Postgres")
    logger.error(e)

def postgres_execute_queries(secret_id, queries):
  try:
    con = postgres_connect(secret_id=secret_id)
    logger.debug(f"Executing {len(queries)} queries on postgres")
    cursor = con.cursor()
    for q in queries:
      logger.debug(f"\n{q}")
      cursor.execute(q)
      con.commit()
    con.close()
  except Exception as e:
    logger.error("Error to execute postgres queries")
    logger.error(e)

