import json
import pandas as pd
from datetime import datetime

from utils.logger import get_logger
from utils.postgres import postgres_to_sql_from_secret
from utils.config import LIFEMED_PG_SECRET_ID, handle_dict

logger = get_logger(__name__)

def register_error(airflow, error):
  try:
    run_spec = handle_dict(airflow)
    
    dag_id = run_spec['DAG_ID']
    task_id = run_spec['TASK_ID']
    run_id = run_spec['RUN_ID']
    error = ' '.join([str(type(error)), str(error)])
    
    error_df = pd.DataFrame(
      data    = [[ dag_id, task_id, run_id, error ]], 
      columns = [ "dag_id", "task_id", "run_id", "error"],
    )
    
    postgres_to_sql_from_secret(
      secret_id = LIFEMED_PG_SECRET_ID,
      df=error_df,
      schema='teste',
      table='airflow_errors'
    )

  except Exception as e:
    logger.error(e)
    return True
