import awswrangler as wr
import pandas as pd

from threading import Thread
from concurrent.futures import ThreadPoolExecutor

from utils.logger import get_logger

logger = get_logger(__name__)

def s3_delete_objects_on_path(s3_path: str):
  try:
    objects = wr.s3.list_objects( path=s3_path )
    if objects:
      wr.s3.delete_objects(objects)
      logger.debug(f"Success to delete files on {s3_path}")
  except Exception as e:
    logger.error(f"An error occurred on delete files on: {s3_path}")
    logger.error(e)

def s3_delete_path(s3_path:str):
  try:
    wr.s3.delete_objects(path=s3_path, use_threads=True)
    logger.debug(f"Success to delete files on {s3_path}")
  except Exception as e:
    logger.error(f"An error occurred on delete files on: {s3_path}")
    logger.error(e)

def s3_delete_paths(s3_paths: list):
  try:

    logger.debug(f"Deleting {len(s3_paths)} paths on s3")
    with ThreadPoolExecutor(max_workers=4) as pool:
      bulk_deletion = tuple(pool.submit(wr.s3.delete_objects, path=args, use_threads = True) for args in s3_paths)
    
    logger.debug(f"Success to delete files on {len(s3_paths)} paths")

  except Exception as e:
    logger.error(f"An error occurred on delete files on: {s3_paths}")
    logger.error(e)

def s3_to_parquet(df: pd.DataFrame, table, database, filename_prefix = None, mode = 'append',partition_cols = None):
  try:
    wr.s3.to_parquet(
      df = df,
      table = table,
      database = database,
      dataset = True,
      compression = 'snappy',
      mode=mode,
      filename_prefix = filename_prefix,
      partition_cols = partition_cols
    )
    logger.debug(f"Success to send files to {database}.{table} s3 table location")
  except Exception as e:
    logger.error(f"An error occurred to save parquet on: {database}.{table}")
    logger.debug(f"DF columns: {df.columns}")
    logger.error(e)

def s3_from_parquet(s3_path):
  return wr.s3.read_parquet(path = s3_path)