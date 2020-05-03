"""
Sagacity Solutions - sagacious.spark.Utils

A set of Spark utility modules for Sagacity Solutions developers
------------------------------------------------------------------------------------------------------------------
Author: Chris Stephenson
Email: managedservice@sagacitysolutions.co.uk
------------------------------------------------------------------------------------------------------------------
"""
# Spark imports
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

class Utils():
  
  
  @staticmethod
  def get_spark_config(config_key):
    """Gets the configuration value set in spark.conf based on the config_key parameter provided. This function
    will suppress any exception thrown if the configuration key/value does not exist in spark.conf. This function
    should therefore be used when the spark.conf option is not mandatory so the caller recieves only a config_val
    of None instead of an exception.

    Args:
      config_key (str): The configuration key to look up from spark.conf

    Returns:
      config_val (str): The configuration value obtained from spark.conf, will be None if the key is not found
    """
    config_val = None

    try:
      config_val = spark.conf.get(config_key)

    except Exception as e:
      pass

    finally:
      return config_val

    
  @staticmethod  
  def get_db_utils():
    """Returns databricks utilities class supporting execution within both notebook and direct execution when
    running on databricks clusters 

    Returns:
      :obj:`pyspark.dbutils`: An instance of the Databricks dbutils class
    """
    dbutils = None
    
    try:
      from pyspark.dbutils import DBUtils
      dbutils = DBUtils(spark)
      
    except ImportError:
      import IPython
      dbutils = IPython.get_ipython().user_ns['dbutils']

    return dbutils
  
  
  @classmethod   
  def get_s3_prefix(cls, aws_access_key=None, aws_secret_key=None, databricks_scope=None):
    """Returns a S3A formated file name prefix for appending to files for read/write in AWS S3 in the format:

      s3a://[aws_access_key]:[aws_secret_key]@

    Access keys are obtained from spark.conf parameters to be supplied to the Spark session. Keys may be submmitted
    to the Spark session directly, or alternatively keys can be obtained from Databricks secrets if this is supported

    Args:
      aws_access_key (str, optional): ...
      aws_secret_key (str, optional): ...
      databricks_scope (str, optional): ...

    Returns:
      str: The S3A file name prefix
    """
    try:  

      # required
      if aws_access_key is None:
        aws_access_key = cls.get_spark_config('sagacious.io.aws.access_key')
        if aws_access_key is None:
          raise Exception('aws_access_key is not defined and is a required parameter!')
        
      if aws_secret_key is None:  
        aws_secret_key = cls.get_spark_config('sagacious.io.aws.secret_key')
        if aws_secret_key is None:
          raise Exception('aws_secret_key is not defined and is a required parameter!')
        
      # optional  
      if databricks_scope is None:   
        databricks_scope = cls.get_spark_config('sagacious.io.databricks_scope')

      # if Databricks screst scope has been defined - fetch keys from secure vault and override original keys
      if databricks_scope is not None:

        dbutils = cls.get_db_utils()

        aws_access_key = dbutils.secrets.get(scope=databricks_scope, key=aws_access_key)
        aws_secret_key = dbutils.secrets.get(scope=databricks_scope, key=aws_secret_key)

      # return formatted s3_prefix 
      return 's3a://{0}:{1}@'.format(aws_access_key, aws_secret_key);

    except Exception as e:
      raise Exception('Error defining AWS S3A file path pre-fix in get_s3_prefix()\n{0}'.format(e))