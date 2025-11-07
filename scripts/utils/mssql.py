import time
import sqlalchemy
import pandas as pd
import numpy as np

from sqlalchemy import create_engine, URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.types import NVARCHAR
from sqlalchemy.engine.base import Connection

from utils.logger import get_logger

logger = get_logger(__name__)

MAX_MSSQL_QUERY_LENGTH = 7800 # 8000 is the real, Set 7800 to overhead on char sum imprecision

def mssql_get_connection(
    host,
    user,
    password,
    port,
    database,
    conn_str_pattern = 'mssql+pymssql://{user}:{password}@{host}:{port}/{database}',
    conn_driver = 'mssql+pymssql',
    retry_limit = 3,
    retry_delay = 30
  ) -> [str, Connection]: # type: ignore
  
  start_server_conn = time.perf_counter()
  conn_str = URL.create(
    drivername=conn_driver,
    username=user,
    password=password,
    host=host,
    port=port,
    database=database
  )
  
  connection = None
  engine = create_engine(conn_str, pool_timeout=70, pool_pre_ping=True)
  
  for i in range(retry_limit):
    try:
      connection = engine.connect()
      elapsed = time.perf_counter() - start_server_conn
      logger.debug(f"({elapsed}s) Elapsed time to connect MSSQL - Retry {i}/{retry_limit}]")
      return connection
    except OperationalError as oe:
      logger.warning(f"Fail to connect MSSQL. Retry {i}/{retry_limit}")
      time.sleep(retry_delay)

  raise OperationalError(f'Fail to connect MSSQL. Retried {retry_limit} times.')

def mssql_get_ip_host(cur_conn):
  try:
    qry = sqlalchemy.text("SELECT CONNECTIONPROPERTY('local_net_address') AS local_net_address")
    c = cur_conn.execute(qry)
    r = c.fetchone()
    return r[0].decode("utf-8")
  except Exception as e:
    logger.error(e)
    logger.error("Fail to get real IP from connection")
    raise e

def mssql_insert_temp_table(cur_df: pd.DataFrame, cur_conn, overwrite, temp_table_name, create_table_query=None) -> int:
  try:
    start = time.perf_counter()
    if(overwrite):
      with cur_conn.begin() as transaction:
        cur_conn.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {temp_table_name};"))
        if(create_table_query):
          cur_conn.execute(sqlalchemy.text(create_table_query))
          
        logger.debug(f"Loading temp Overwriting")
    
    cur_df.to_sql( temp_table_name,  con=cur_conn,  if_exists='append',  index=False,  method='multi',  chunksize = 900 )
    
    elapsed = time.perf_counter() - start
    with cur_conn.begin() as transaction:
      temp_table_row_count_cursor = cur_conn.execute(sqlalchemy.text(f"select count(1) from {temp_table_name};"))
      temp_table_row_count = (temp_table_row_count_cursor.fetchall())[0]
    
    logger.debug(f"({elapsed}s) Loaded {temp_table_row_count} rows into Temp ChunkDF")
    elapsed = time.perf_counter() - start
    logger.debug(f"({elapsed}s) Checked Temp ChunkDF")
    return temp_table_row_count
  except Exception as e:
    logger.error(f"Fail to load temp table")
    logger.error(e)
    
def mssql_query_to_dataframe(cur_conn, qry) -> [pd.DataFrame, int]: # type: ignore
  try:
    start = time.perf_counter()
    df_enriched_from_mssql = pd.read_sql(qry, cur_conn)
    elapsed = time.perf_counter() - start
    logger.debug(f"({elapsed}s) Success executed query on MSSQL: \n{qry}")
    return df_enriched_from_mssql
  except Exception as e:
    logger.error(f"Fail to extract from MSSQL")
    logger.error(e)
    raise e

def mssql_get_generator(connection, mssql_query, chunk_size=None):
  try:
    start = time.perf_counter()
    
    df_generator = pd.read_sql(
      con = connection,
      sql = mssql_query,
      chunksize= chunk_size
    )
    
    elapsed = time.perf_counter() - start
    logger.debug(f"({elapsed}s) Success executed query on MSSQL: \n{mssql_query}")
    logger.info(f"MSSQL on generator took: {elapsed}")
    return df_generator
  except Exception as e:
    logger.error(f"Fail to extract from MSSQL server.")
    logger.error(e)
    return None

def build_open_query(raw_query, linked_server):
  query = raw_query.replace("'", "''").replace('\t', '').replace('  ', ' ')
  parsed_query = f"""SELECT *
  FROM OPENQUERY([{linked_server}],
  '{query}'
  )
  """
  return parsed_query

def build_cte_values_query(df, query):
  cte_pattern = "SELECT * FROM (VALUES\n  {str_values}\n) AS t ({columns_sql})"
  
  query_len = len(query)

  columns = df.columns.tolist()
  columns_sql = ", ".join([f"[{col}]" for col in columns])
  
  queries = []
  values_sql_parts = []
  temp_str_count = 0
  for row in df.itertuples(index=False):
    row_values = []
    for value in row:
      if pd.isnull(value):
        row_values.append('NULL')
      elif isinstance(value, str):
        escaped_value = f"''{value}''"
        row_values.append(escaped_value)
      elif(isinstance(value, (np.bool_, bool, pd.BooleanDtype))):
        row_values.append(str(int(value)))
      else:
        row_values.append(str(value))
    
    str_row = '(' + ', '.join(row_values) + ')'
    temp_str_count += len(str_row) + 4 # + 4 Because of the size of ",\n  ", which is used in the join of these values
    
    if( ( len(cte_pattern) + temp_str_count + query_len )  >= MAX_MSSQL_QUERY_LENGTH):
      values_sql = f"SELECT * FROM (VALUES\n  " + ",\n  ".join(values_sql_parts) + f"\n) AS t ({columns_sql})"
      sql = query.format( temp_table_query = values_sql)
      queries.append(sql)
      values_sql_parts = []
      temp_str_count = 0
    
    values_sql_parts.append(str_row)
  
  if(len(values_sql_parts) > 0):
    values_sql = f"SELECT * FROM (VALUES\n  " + ",\n  ".join(values_sql_parts) + f"\n) AS t ({columns_sql})"
    sql = query.format( temp_table_query = values_sql)
    queries.append(sql)
  
  return queries
