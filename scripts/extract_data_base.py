import pandas as pd
import sys
import os
import time
from uuid import uuid4
from functools import reduce

from datetime import datetime, timedelta
from awswrangler.exceptions import QueryFailed

from utils.config import handle_env_vars, get_provider_secret, handle_default_vars, PROJECT_PATH
from utils.mssql import mssql_get_connection, mssql_query_to_dataframe, mssql_insert_temp_table, build_open_query, build_cte_values_query
from utils.s3 import s3_delete_paths, s3_to_parquet, s3_from_parquet, s3_delete_path
from utils.athena import athena_execute_query, athena_get_generator
from utils.logger import get_logger

logger = get_logger('medications')

# Env Vars
__ENV_VARS = [
    'PROVIDER',
    'LOGICAL_DATE',
    'MEDITECH_SECRET_ID'
]

__OPT_ENV_VARS = [
    'MEDITECH_MSSQL_SERVER_NAME',
    'ATHENA_OUTPUT_DB',
    'ATHENA_OUTPUT_TABLE',
    'NEW_ACCOUNTS_SCHEMA',
    'NEW_ACCOUNTS_TABLE',
    'BATCH_ID',
    'SERVICE_NAME',
    'LINKED_SERVER',
    'CUSTOM_INPUT_QUERY',
    'CUSTOM_INPUT_PARQUET_S3_PATH'
]



if __name__ == '__main__':
  try:
    start_time = time.perf_counter()
    
    # Provider config

    PROVIDER = handle_env_vars(
      required  = __ENV_VARS,
      optional  = __OPT_ENV_VARS
    )

    PROVIDER = handle_default_vars(
      env_vars    = PROVIDER,
      runner      = 'xxxxxxxxxx'
    )

    # Consts
    ATHENA_CHUNK_SIZE =  300
    MSSQL_CHUNK_RATE = 1
    MSSQL_DATABASES = PROVIDER['xxxxxxxxxxxxxx']

    MSSQL_TEMP_TABLE_NAME = "#xxxxxxxxxxxxxxxxx"
    
    OUTPUT_S3_LOCATION_TEMPLATE = 'xxxxxxxxxxxx/{DATABASE}/{TABLE}'
    OUTPUT_S3_LOCATION = OUTPUT_S3_LOCATION_TEMPLATE.format( DATABASE = PROVIDER['ATHENA_OUTPUT_DB'], TABLE = PROVIDER['ATHENA_OUTPUT_TABLE'] )
    
    ATHENA_QUERY_OUTPUT_S3_PATH:str = 'xxxxxxxxxxxxxxxxxxxxxxxx'
    
    SUB_PROVIDER = PROVIDER.get('SUB_PROVIDER', False)
    LINKED_SERVER = PROVIDER.get('LINKED_SERVER', False)
    
    CUSTOM_DATA_MANDATORY_COLUMNS = ['xxxxxxxxxx']
    CUSTOM_DATA_DB = 'xxxxxxxxxxxx'
    CUSTOM_DATA_TABLE = f'{PROVIDER.get('ATHENA_OUTPUT_TABLE')}_custom_{str(uuid4())[:8]}'
    CUSTOM_DATA_S3_PATH = OUTPUT_S3_LOCATION_TEMPLATE.format( DATABASE = CUSTOM_DATA_DB, TABLE = CUSTOM_DATA_TABLE )
    CUSTOM_DATA_DDL = open(os.path.join(PROJECT_PATH, PROVIDER['QUERIES']['CREATE_CUSTOM_TABLE_FILE']), 'r').read().format(
      ATHENA_OUTPUT_DB = CUSTOM_DATA_DB,
      ATHENA_OUTPUT_TABLE = CUSTOM_DATA_TABLE,
      OUTPUT_S3_LOCATION = CUSTOM_DATA_S3_PATH
    )

    
    logger.info(f"Start")
    
    with open( os.path.join(PROJECT_PATH, PROVIDER['QUERIES']['CREATE_OUTPUT_TABLE_FILE'] ) , 'r') as f:
      lines = f.readlines()
      partition_names = [partition.split()[0] for partition in  PROVIDER['PARTITIONS']]
      
      for i in range(len(lines)):
        for p in partition_names:
          if( p in lines[i]):
            if(lines[i+1].strip() == ')'):
              lines[i] = lines[i].replace(',', '')
            else:
              lines[i] = ''
              
      qry_create_athena_table = ''.join(lines)
      qry_create_athena_table = qry_create_athena_table.format(
        ATHENA_OUTPUT_DB = PROVIDER['ATHENA_OUTPUT_DB'],
        ATHENA_OUTPUT_TABLE = PROVIDER['ATHENA_OUTPUT_TABLE'],
        OUTPUT_S3_LOCATION = OUTPUT_S3_LOCATION,
        PARTITIONS = ','.join(PROVIDER['PARTITIONS'])
      )

    account_number_file_path = os.path.join(PROJECT_PATH, PROVIDER['QUERIES']['ACCOUNT_NUMBERS_ATHENA_UNION'])
    with open( account_number_file_path , 'r' ) as f:
      qry_account_numbers_union = ''.join(f.readlines())
      EXTRA_PARAMS = ''

      if(PROVIDER['REALTIME']):
        EXTRA_PARAMS += f" AND e.batch = '{PROVIDER['BATCH_ID']}'"
        logger.info(f"Processing realtime batch: {PROVIDER['BATCH_ID']}")
      else:
        EXTRA_PARAMS += f""
        
      qry_account_numbers_union = qry_account_numbers_union.format(
        provider = PROVIDER['PROVIDER'],
        LOGICAL_DATE = PROVIDER.get('LOGICAL_DATE'),
        NEW_ACCOUNTS_SCHEMA = PROVIDER['NEW_ACCOUNTS_SCHEMA'],
        NEW_ACCOUNTS_TABLE = PROVIDER['NEW_ACCOUNTS_TABLE'],
        ATHENA_OUTPUT_DB = PROVIDER['ATHENA_OUTPUT_DB'],
        ATHENA_OUTPUT_TABLE = PROVIDER['ATHENA_OUTPUT_TABLE'],
        SERVER_NAME = PROVIDER.get('MEDITECH_MSSQL_SERVER_NAME', 'DEFAULT'),
        SERVICE_NAME = PROVIDER['SERVICE_NAME'],
        extra_filters = EXTRA_PARAMS,
        CUSTOM_DATABASE = CUSTOM_DATA_DB,
        CUSTOM_TABLE = CUSTOM_DATA_TABLE,
      )

    with open( os.path.join(PROJECT_PATH, PROVIDER['QUERIES']['MEDICAL_RECORDS_MEDICATIONS']) , 'r' ) as f:
      qry_medical_records_medications = ''.join(f.readlines())
      
      database_name_one = MSSQL_DATABASES[0]
      
      if(LINKED_SERVER):
        qry_medical_records_medications = build_open_query(
          raw_query = qry_medical_records_medications,
          linked_server = LINKED_SERVER
        )
      
      qry_medical_records_medications = qry_medical_records_medications.format(
        provider = PROVIDER['PROVIDER'],
        mssql_temp_table_name = MSSQL_TEMP_TABLE_NAME,
        mssql_database_name = database_name_one,
        temp_table_query = '{temp_table_query}',
        LOGICAL_DATE = PROVIDER['LOGICAL_DATE']
      )

    with open( os.path.join(PROJECT_PATH, PROVIDER['QUERIES']['CREATE_MSSQL_TEMP_TABLE']) , 'r' ) as f:
      create_mssql_temp_table = ''.join(f.readlines())
      
      mssql_collate = f"COLLATE {PROVIDER.get('MSSQL_COLLATE')}" if PROVIDER.get('MSSQL_COLLATE', False) else ''
      
      create_mssql_temp_table = create_mssql_temp_table.format(
        mssql_temp_table_name = MSSQL_TEMP_TABLE_NAME,
        collate  = mssql_collate
      )


    HOST, PORT, DATABASE, USER, PASSWORD = get_provider_secret(
      provider = PROVIDER['PROVIDER'],
      meditech_secret_id = PROVIDER['MEDITECH_SECRET_ID'],
      meditech_mssql_server_name = PROVIDER.get('MEDITECH_MSSQL_SERVER_NAME')
    )
    
    custom_parquet_df = pd.DataFrame(columns=CUSTOM_DATA_MANDATORY_COLUMNS)
    custom_query_df = pd.DataFrame(columns=CUSTOM_DATA_MANDATORY_COLUMNS)

    CUSTOM_INPUT_QUERY = PROVIDER.get('CUSTOM_INPUT_QUERY', None)
    
    if(CUSTOM_INPUT_QUERY):
      custom_query_df = athena_get_generator(athena_query=CUSTOM_INPUT_QUERY) # type: ignore
      custom_df_ok_columns = reduce(lambda x, y: (y in custom_query_df.columns) and x, CUSTOM_DATA_MANDATORY_COLUMNS, True) # type: ignore

      if(not custom_df_ok_columns):
        raise ValueError( f"Wrong custom query columns on 'CUSTOM_INPUT_QUERY'. \nExpected:{CUSTOM_DATA_MANDATORY_COLUMNS} \nReceived: {str(list(custom_query_df.columns))}" ) # type: ignore
    custom_parquet_s3_path = PROVIDER.get('CUSTOM_INPUT_PARQUET_S3_PATH', None)
    
    if(custom_parquet_s3_path):
      custom_parquet_df = s3_from_parquet(s3_path=custom_parquet_s3_path)
      custom_parquet_df['is_new'] = True
      custom_df_ok_columns = reduce(lambda x, y: (y in custom_parquet_df.columns) and x, CUSTOM_DATA_MANDATORY_COLUMNS, True)

      if(not custom_df_ok_columns):
        raise ValueError( f"Wrong custom dataframe columns on 'CUSTOM_INPUT_PARQUET_S3_PATH'. \nExpected:{CUSTOM_DATA_MANDATORY_COLUMNS} \nReceived: {str(list(custom_parquet_df.columns))}" )

    custom_df = pd.concat([custom_parquet_df[CUSTOM_DATA_MANDATORY_COLUMNS], custom_query_df[CUSTOM_DATA_MANDATORY_COLUMNS]]) # type: ignore

    logger.info(f"Creating custom data temp table {PROVIDER['ATHENA_OUTPUT_DB']}.{PROVIDER['ATHENA_OUTPUT_TABLE']} on athena if not exists")
    athena_execute_query(query=CUSTOM_DATA_DDL)
    
    if(not custom_df.empty):
      s3_to_parquet(
        df = custom_df,
        database = CUSTOM_DATA_DB,
        table = CUSTOM_DATA_TABLE,
        mode = 'overwrite'
      )
    
    del custom_df
    del custom_parquet_df
    del custom_query_df


    logger.info(f"Creating table {PROVIDER['ATHENA_OUTPUT_DB']}.{PROVIDER['ATHENA_OUTPUT_TABLE']} on athena if not exists")
    athena_execute_query(query=qry_create_athena_table)
    
    
    athena_generator = athena_get_generator(
      athena_query=qry_account_numbers_union,
      chunk_size= ATHENA_CHUNK_SIZE
    )
        
    if(athena_generator is None):
      logger.info('No data found in Athena.')
      s3_delete_path( s3_path=CUSTOM_DATA_S3_PATH )
      athena_execute_query(f'DROP TABLE {CUSTOM_DATA_DB}.{CUSTOM_DATA_TABLE}')
      sys.exit(0)
      
      
    total_athena_rows = 0
    total_mssql_rows = 0

    athena_chunk_iteration = 0

    has_next = True
    
    s3_file_prefix_to_old_accounts = f"{PROVIDER.get('MEDITECH_MSSQL_SERVER_NAME', PROVIDER['PROVIDER'])}_{PROVIDER['LOGICAL_DATE']}_"
    s3_file_prefix_to_new_accounts = f"{PROVIDER.get('MEDITECH_MSSQL_SERVER_NAME', PROVIDER['PROVIDER'])}_"
    partition_to_batch = [partition.split()[0] for partition in  PROVIDER['PARTITIONS']]
    new_claims = 0
    old_claims = 0

    s3_deleted_paths = []

    while(has_next):
      with mssql_get_connection (
        host              = HOST,
        user              = USER,
        password          = PASSWORD,
        port              = PORT,
        database          = DATABASE,
        conn_str_pattern  = PROVIDER['CONN_STR'],
        retry_delay = 10,
        retry_limit = 5
      ) as conn:
        
        s3_paths_to_delete = []
            
        mssql_chunk_iteration = 0
        mssql_temp_overwrite = True
        
        if(not LINKED_SERVER):
          mssql_query = qry_medical_records_medications.format(temp_table_query = f'select * from {MSSQL_TEMP_TABLE_NAME}')
          
          for i in range (MSSQL_CHUNK_RATE):
            try:
              athena_df = athena_generator.__next__()
              athena_df['is_new'] = athena_df['is_new'].astype(int)
              total_athena_rows += athena_df.shape[0]
              mssql_insert_temp_table(athena_df, conn, mssql_temp_overwrite, MSSQL_TEMP_TABLE_NAME, create_mssql_temp_table)
              mssql_temp_overwrite = False
              mssql_chunk_iteration += 1
              logger.info(f"Chunk MSSQL Iteration {mssql_chunk_iteration}/{MSSQL_CHUNK_RATE}")
              logger.info(f"{athena_df.shape[0]} sended to MSSQL temp table")
            except  StopIteration as end_input_athena:
              logger.info(f"End Athena Extract")
              has_next = False
              break
          
          if(mssql_chunk_iteration > 0):
            mssql_df_all = mssql_query_to_dataframe(  
              cur_conn = conn,
              qry = mssql_query
            )

        else:
          temp_table_queries = []
          try:
            athena_df = athena_generator.__next__()
            temp_table_queries = build_cte_values_query(athena_df, qry_medical_records_medications)
            total_athena_rows += athena_df.shape[0]
            new_claims += athena_df[athena_df['is_new'] == 1].shape[0]
            old_claims += athena_df[athena_df['is_new'] == 0].shape[0]
            
          except:
            logger.info(f"End Athena Extract. {total_athena_rows} rows")
            has_next = False
          
          for index, mssql_cte_query in enumerate(temp_table_queries):
            if( mssql_chunk_iteration == 0 ):
              mssql_df_all = mssql_query_to_dataframe( cur_conn = conn, qry = mssql_cte_query )
            else:
              mssql_df_all = pd.concat([mssql_df_all, mssql_query_to_dataframe( cur_conn = conn, qry = mssql_cte_query )])
            mssql_chunk_iteration += 1

        t1 = time.perf_counter()
      
        if(SUB_PROVIDER):
          mssql_df_all['sub_provider'] = SUB_PROVIDER
        

      s3_paths_to_delete = []
      if( isinstance(mssql_df_all[mssql_df_all['is_new'] == 1], pd.DataFrame) and mssql_chunk_iteration > 0):
        total_mssql_rows += mssql_df_all[mssql_df_all['is_new'] == 1].shape[0]
        logger.info(f"{mssql_df_all[mssql_df_all['is_new'] == 1].shape[0]} extracted from Meditech MSSQL all comments")
        
        if(not mssql_df_all[mssql_df_all['is_new'] == 1].empty):
          
          for index, row in mssql_df_all[mssql_df_all['is_new'] == 1][partition_to_batch].drop_duplicates().iterrows():
            str_row = [str(r) for r in row]
            s3_path_partitions = [ '='.join(i) for i in zip(partition_to_batch, str_row)]
            s3_path = '/'.join([OUTPUT_S3_LOCATION] + s3_path_partitions + [s3_file_prefix_to_new_accounts])

            if (s3_path not in s3_paths_to_delete and s3_path not in s3_deleted_paths):
              s3_paths_to_delete.append(s3_path)
          
          s3_delete_paths(s3_paths_to_delete)

          s3_deleted_paths += s3_paths_to_delete

          if(not mssql_df_all[mssql_df_all['is_new'] == 1].empty): 
            s3_to_parquet(
              df = mssql_df_all[mssql_df_all['is_new'] == 1][[i for i in mssql_df_all.columns if i != 'is_new']],
              table = PROVIDER['ATHENA_OUTPUT_TABLE'],
              database = PROVIDER['ATHENA_OUTPUT_DB'],
              filename_prefix = s3_file_prefix_to_old_accounts,
              mode = 'append',
              partition_cols = partition_to_batch
            )


      s3_paths_to_delete = []
      if( isinstance(mssql_df_all[mssql_df_all['is_new'] == 0], pd.DataFrame) and mssql_chunk_iteration > 0):
        total_mssql_rows += mssql_df_all[mssql_df_all['is_new'] == 0].shape[0]
        logger.info(f"{mssql_df_all[mssql_df_all['is_new'] == 0].shape[0]} extracted from Meditech MSSQL new comments")
        
        for index, row in mssql_df_all[mssql_df_all['is_new'] == 0][partition_to_batch].drop_duplicates().iterrows():
          str_row = [str(r) for r in row]
          s3_path_partitions = [ '='.join(i) for i in zip(partition_to_batch, str_row)]
          s3_path = '/'.join([OUTPUT_S3_LOCATION] + s3_path_partitions + [s3_file_prefix_to_old_accounts])

          if (s3_path not in s3_paths_to_delete and s3_path not in s3_deleted_paths):
            s3_paths_to_delete.append(s3_path)

        s3_delete_paths(s3_paths_to_delete)

        s3_deleted_paths += s3_paths_to_delete

        if(not mssql_df_all[mssql_df_all['is_new'] == 0].empty):
          s3_to_parquet(
            df = mssql_df_all[mssql_df_all['is_new'] == 0][[i for i in mssql_df_all.columns if i != 'is_new']],
            table = PROVIDER['ATHENA_OUTPUT_TABLE'],
            database = PROVIDER['ATHENA_OUTPUT_DB'],
            filename_prefix = s3_file_prefix_to_old_accounts,
            mode = 'append',
            partition_cols = partition_to_batch
          )
          
      athena_chunk_iteration += 1
      elapsed = f"{str(time.perf_counter()-start_time)[:5]}s"

    s3_delete_path( s3_path=CUSTOM_DATA_S3_PATH )
    athena_execute_query(f'DROP TABLE {CUSTOM_DATA_DB}.{CUSTOM_DATA_TABLE}')

    logger.info(f"""\n
    Athena    input size: {total_athena_rows}
    Meditech  output size: {total_mssql_rows}  
    
    New Claims    input size: {new_claims}
    Old Claims    input size: {old_claims}
    """)
    
    elapsed = f"{str(time.perf_counter()-start_time)[:5]}s"
    logger.info(f"Elapsed time: {elapsed}")

    logger.info("Success")
  except Exception as e:
    s3_delete_path( s3_path=CUSTOM_DATA_S3_PATH )
    athena_execute_query(f'DROP TABLE {CUSTOM_DATA_DB}.{CUSTOM_DATA_TABLE}')    
    logger.error(e)
    sys.exit(0)