import boto3
from botocore.exceptions import ClientError

from utils.logger import get_logger

logger = get_logger(__name__)

def get_secret( secret_name:str, region_name:str = "us-east-1" ):
  logger.debug("Boto3 session")
  session = boto3.session.Session()
  
  logger.debug("Secret manager client")
  client = session.client( service_name='secretsmanager', region_name=region_name )
  
  try:
    logger.debug("Getting secrets")
    get_secret_value_response = client.get_secret_value(
      SecretId=secret_name
    )
  except ClientError as e:
    raise e
  secret = get_secret_value_response['SecretString']
  return secret
