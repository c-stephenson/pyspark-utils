"""
Sagacity Solutions - sagacious.spark.IO

Manages reading and writng to cloud storage Azure Blob Storage (ABS) and AWS S3 (S3) and Databricks File
System (DBFS)

Reading and writing data should to cloud storage should normally be done normally via spark.read and 
dataframe.write methods for working with spark data. However the IO class is intended to  be used for reading 
and writing regular files to cloud storage e.g. as reading configuration files and writing log files.
------------------------------------------------------------------------------------------------------------------
Author: Chris Stephenson
Email: managedservice@sagacitysolutions.co.uk
------------------------------------------------------------------------------------------------------------------
"""
# Spark imports
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# from sagacious.spark.Utils import * as sgf
import os
from ntpath import basename
from cloudstorage.drivers.amazon import S3Driver
from cloudstorage.drivers.microsoft import AzureStorageDriver


class IO():
  """Manages reading and writng to cloud storage Azure Blob Storage (ABS) and AWS S3 (S3) and Databricks File
  System (DBFS)

  Reading and writing data should to cloud storage should normally be done normally via spark.read and 
  dataframe.write methods for working with spark data. However the IO class is intended to  be used for reading 
  and writing regular files to cloud storage e.g. as reading configuration files and writing log files.

  Args:
    default_storage_type (str, optional): The default cloud storage type for read/write operations. Either 'ABS' 
      ,'S3' or 'DBFS' are supported. If argument is not supplied IO will attempt to obtain this from the 
      spark.conf 'sagacious.io.default_storage_type' value.

  Attributes:  
    default_storage_type (str): The default cloud storage type for read/write operations. Either 'ABS' or 'S3' or
    'DBFS'

  Examples:
    >>> vbm_io = IO('ABS')
    >>> some_bytes = vbm_io.read_bytes(file_name='tmp/test.txt')
    >>> vbm_io.write_bytes(file_bytes=some_other_bytes, file_name='tmp/test.txt')   
  """
  
  # class constructor
  def __init__(self, default_storage_type=None, aws_access_key=None, aws_secret_key=None, aws_region=None, azure_account=None, azure_credential=None, databricks_scope=None):
  
    # init private properties
    self.__abs_driver = None
    self.__s3_driver = None
    
    self.__aws_access_key = aws_access_key
    self.__aws_secret_key = aws_secret_key
    self.__aws_region = aws_region
    
    self.__azure_account = azure_account
    self.__azure_credential = azure_credential
    
    self.__databricks_scope = databricks_scope
    
    # init public properties
    self.default_storage_type = default_storage_type
    self.storage_container = None

    try:
      
      # if default_storage_type is not provided on initialisation, check if this is specified in configuration 
      # sagacious.io.default_storage_type
      if self.default_storage_type is None:
        self.default_storage_type = Utils.get_spark_config('sagacious.io.default_storage_type')
        
        # if still not provided, fall back to default of Databricks File System 'DBFS'
        if self.default_storage_type is None:
          self.default_storage_type = 'DBFS'
      
    except Exception as e:
      raise Exception("Unable to locate required spark.conf parameters 'sagacious.io.default_storage_type'\n{0}".format(e))
      
    if self.default_storage_type == 'DBFS':
      # Databricks Files System - no initialisation required
      pass

    elif self.default_storage_type == 'S3':
      # AWS S3
      self.__s3_driver = self.__init_aws_conn()

    elif self.default_storage_type == 'ABS':
      # MS Azure  
      self.__abs_driver = self.__init_azure_conn()

    else:
      raise Exception("Storage type '" + self.default_storage_type + "' not supported! Please select DBFS, S3, or ABS\n{0}".format(e))
  
  
  # read file bytes from 'file_name' @ 'storage_location'
  def read_bytes(self, file_name, storage_container=None, storage_type=None):
    """Reads the file content on the cloud storage returning the file bytes
    
    vbm.default_storage_container
    
    Note:
      Will use default values for args storage_container and storage_type if not defined. The default storage
      container will be obtained from the spark.conf sagacious.io.default_storage_container parameter.

    Args:
      file_name (str): The file name including path prefixes (or full blob name) to be read
      storage_container (str, optional): The storage container name where the blob resides, ABS container name or 
        bucket name for S3, not required for DBFS storage-type
      storage_type (str, optional): The type of storage where the blob resides, either 'ABS' for Azure Blob Storage 
        ,'S3' for AWS S3 storage or 'DBFS' if using the Databricks file system (including cloud store mount points)

    Returns:
      bytes: The bytes contained in the file read
    """
    if storage_type is None:
      storage_type = self.default_storage_type
      
    if storage_container is not None:
      self.storage_container = storage_container

    if self.storage_container is None:
      self.storage_container = Utils.get_spark_config('sagacious.io.default_storage_container')
      
    # DBFS
    if storage_type.strip().upper() == 'DBFS':
      
      # reading from DBFS
      file_name = file_name.lower().strip('/')
      # Requires a DBFS prefix, remove (if present) & (re-)append
      file_name = file_name.lower().strip('dbfs/')
      dbfs_prefix = '/dbfs'

      file = os.path.join(dbfs_prefix, file_name)

    else:  
      # AWS S3, Azure ABS
      if self.storage_container is None:
        # For S3 & ABS storage_container is required
        raise Exception("The storage_container must be specified for storage_type '" + storage_type.strip().upper() + "'")
        
      if storage_type.strip().upper() == 'S3':

        # if the S3 driver hasn't already been intialised - do it now
        if self.__s3_driver is None:
          self.__s3_driver = self.__init_aws_conn()

        container = self.__s3_driver.get_container(self.storage_container)
        blob = self.__s3_driver.get_blob(container, file_name.strip("/"))

      # Azure Blob Storage
      if storage_type.strip().upper() == 'ABS':

        # if the driver hasn't already been intialised - do it now
        if self.__abs_driver is None:
          self.__abs_driver = self.__init_azure_conn()

        container = self.__abs_driver.get_container(self.storage_container)
        blob = self.__abs_driver.get_blob(container, file_name.strip("/"))

      # load in and download files
      file = basename(file_name.strip("/"))
      blob.download(file)
 
    # read in file
    f = None
    try:
      f = open(file, 'rb') 

      # return file bytes
      fbytes = f.read()
      
      return fbytes
    
    finally:
      f.close()
      
  
  def write_bytes(self, file_bytes, file_name, storage_container=None, storage_type=None):
    """Write bytes content into the cloud storage as a blob
    
    Note:
      Will use default values for args storage_container and storage_type if not defined. The default storage
      container will be obtained from the spark.conf vbm.default_storage_container parameter.

    Args:
      file_bytes (bytes): The bytes to be written
      file_name (str): The file name including path prefixes (or full blob name) to be read
      storage_container (str, optional): The storage container name where the blob resides, ABS container name or 
        bucket name for S3, not required for DBFS storage-type
      storage_type (str, optional): The type of storage where the blob resides, either 'ABS' for Azure Blob Storage 
        ,'S3' for AWS S3 storage or 'DBFS' if using the Databricks file system (including cloud store mount points)
    """
    try:
      
      if storage_type == None:
        storage_type = self.default_storage_type

      if storage_container is not None:
        self.storage_container = storage_container
      
      if self.storage_container is None:
        self.storage_container = Utils.get_spark_config('sagacious.io.default_storage_container')

      if storage_type.strip().upper() == 'DBFS':
        # writing to DBFS
        file_name = file_name.lower().strip('/')
        # Requires a DBFS prefix, remove (if present) & (re-)append
        file_name = file_name.lower().strip('dbfs/')
        dbfs_prefix = '/dbfs'

        file_name = os.path.join(dbfs_prefix, file_name)

        with open(file_name, 'wb') as wf:
          wf.write(file_bytes)
        
      else:
        # AWS S3, Azure ABS
        if self.storage_container is None:
          # For S3 & ABS storage_container is required
          raise Exception("The storage_container must be specified for storage_type '" + storage_type.strip().upper() + "'")
          
        if storage_type.strip().upper() == 'S3':

          # if the driver hasn't already been intialised - do it now
          if self.__s3_driver is None:
            self.__s3_driver = self.__init_aws_conn()

          container = self.__s3_driver.get_container(self.storage_container)

        # Azure Blob Storage
        if storage_type.strip().upper() == 'ABS':

          # if the driver hasn't already been intialised - do it now
          if self.__abs_driver is None:
            self.__abs_driver = self.__init_azure_conn()

          container = self.__abs_driver.get_container(self.storage_container)

        # due to limitations of cloudstorage upload_blob functionailty we must
        # write out our stream locally and upload from there
        temp_file_name = '/tmp/sagacious_io_buffer.dat'

        # write to local buffer
        with open(temp_file_name, 'wb') as wf:
          wf.write(file_bytes)

        # upload the file
        file_name = file_name.strip("/")
        file_container = container.upload_blob(filename=temp_file_name, blob_name=file_name)  

        # remove the file
        os.remove(temp_file_name)
      
    except Exception as e:
      raise Exception('Unable to write file {0}\n{1}'.format(file_name, e))
  
  
  def __init_aws_conn(self):
  # initialises and returns the AWS driver from cloudstorage
    
    # required. params
    if self.__aws_access_key is None:
      # No azure_credential provided, check spark config
      self.__aws_access_key = Utils.get_spark_config('sagacious.io.aws.access_key')
      # if not successful throw exception, parameter is required!
      if self.__aws_access_key is None:
        raise Exception("Unable to initialise connection to AWS S3, required parameter 'aws_access_key'")
        
    if self.__aws_secret_key is None:
      # No azure_credential provided, check spark config
      self.__aws_secret_key = Utils.get_spark_config('sagacious.io.aws.secret_key')
      # if not successful throw exception, parameter is required!
      if self.__aws_secret_key is None:
        raise Exception("Unable to initialise connection to AWS S3, required parameter 'aws_secret_key'")
        
    # optional params
    if self.__databricks_scope is None:
      self.__databricks_scope = Utils.get_spark_config('sagacious.io.databricks_scope')
      
    if self.__aws_region is None:
      self.__aws_region = Utils.get_spark_config('sagacious.io.aws.region')
    
    try:
      
      if self.__databricks_scope is not None:
        # fetch the actual credential from databrick vault
        dbutils = Utils.get_db_utils()
        aws_access_key = dbutils.secrets.get(scope=self.__databricks_scope, key=self.__aws_access_key)
        aws_secret_key = dbutils.secrets.get(scope=self.__databricks_scope, key=self.__aws_secret_key)
      else:
        # else creds supplied directly
        aws_access_key = self.__aws_access_key
        aws_secret_key = self.__aws_secret_key
      
      # init driver
      if self.__aws_region is None:
        s3_driver = S3Driver(key=aws_access_key, secret=aws_secret_key)
      else:
        # region is optional but may be required by AWS in some circumstances
        s3_driver = S3Driver(key=aws_access_key, secret=aws_secret_key, region=self.__aws_region)
      
    except Exception as e:
      raise Exception('Unable to initialise connection to AWS S3 storage\n{0}'.format(e))
      
    return s3_driver
  

  def __init_azure_conn(self):  
  # initialises and returns the Azure driver from cloudstorage
  
    # required. params
    if self.__azure_account is None:
      # No azure_credential provided, check spark config
      self.__azure_account = Utils.get_spark_config('sagacious.io.azure.account')
      # if not successful throw exception, parameter is required!
      if self.__azure_account is None:
        raise Exception("Unable to initialise connection to Azure blob storage, required parameter 'azure_account'")
            
    if self.__azure_credential is None:
      # No azure_credential provided, check spark config
      self.__azure_credential = Utils.get_spark_config('sagacious.io.azure.credential')
      # if not successful throw exception, parameter is required!
      if self.__azure_credential is None:
        raise Exception("Unable to initialise connection to Azure blob storage, required parameter 'azure_credential'")
  
    # optional param
    if self.__databricks_scope is None:
      self.__databricks_scope = Utils.get_spark_config('sagacious.io.databricks_scope')
  
    try:

      if self.__databricks_scope is not None:
        # fetch the actual credential from databrick vault
        dbutils = Utils.get_db_utils()
        credential = dbutils.secrets.get(scope=self.__databricks_scope, key=self.__azure_credential)
      else:
        # else creds supplied directly
        credential = self.__azure_credential

      # init driver
      abs_driver = AzureStorageDriver(account_name=self.__azure_account, key=credential)
      
    except Exception as e:
      raise Exception('Unable to initialise connection to Azure storage\n{0}'.format(e))
      
    return abs_driver 

