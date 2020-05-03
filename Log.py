"""
Sagacity Solutions - sagacious.spark.Log

An implementation of python logging for use when logs are required to be written directly to cloud based blob
storage. sagacious.spark.Log provides a 'wrapper' of python Logging with use of sagacious.spark.IO to support
writing of logs to blob storage

Log currently supports the following storage_type's:
- S3   : Amazon S3
- ABS  : Azure Blob Storage
- DBFS : Databricks File System - Directly mounted cloud storage via DBFS for running on Databricks Spark clusters
------------------------------------------------------------------------------------------------------------------
Author: Chris Stephenson
Email: managedservice@sagacitysolutions.co.uk
------------------------------------------------------------------------------------------------------------------
"""
# Spark imports
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#import os 
import logging
import io
from datetime import datetime
import sys

#from Utils import * as sgf #sagacious.spark.Utils


class Log():
  """An implementation of python logging for use when logs are required to be written directly to cloud based blob
  storage.
  Log currently supports the following storage_type's:
  - S3   : Amazon S3
  - ABS  : Azure Blob Storage
  - DBFS : Databricks File System - Directly mounted cloud storage via DBFS for running on Databricks Spark clusters

  Args:
    process_name (str, optional): The process i.e. the VBM module being logged
    log_level (str, optional): The standard Python log log levels i.e. INFO, WARNING, ERROR, CRITICAL 
    storage_container (str, optional): The storage container name where the blob resides, ABS container name or 
      bucket name for S3, not required for DBFS storage-type
    storage_type (str, optional): The type of storage where the blob resides, either 'ABS' for Azure Blob Storage 
      ,'S3' for AWS S3 storage or 'DBFS' if using the Databricks file system (including cloud store mount points)
    file_path (str, optional): the file path (blob file prefix) to where the log file will be written
    to_stdout (bool, optional): True if the log is also to be written to stdout (console), False if logging to 
      file only
      
  Attributes:  
    log (:obj:`log`): The Python log object is accessible from this attribute 
    process_name (str): The process i.e. the VBM module being logged
    log_level (str): The standard Python log log levels i.e. INFO, WARNING, ERROR, CRITICAL 
    storage_container (str, optional): The storage container name where the blob resides, ABS container name or 
      bucket name for S3, not required for DBFS storage-type
    storage_type (str, optional): The type of storage where the blob resides, either 'ABS' for Azure Blob Storage 
      ,'S3' for AWS S3 storage or 'DBFS' if using the Databricks file system (including cloud store mount points)
    file_path (str): the file path (blob file prefix) to where the log file will be written
    log_file_name (str): the log file to where the log buffer will be written, with the file name
      format [process_name]_[%Y%m%d]_[%H%M%S].log

  Examples:
    >>> vbm_log = Log(process_name='VBM_TENURE', log_level='WARNING')
    >>> vbm_log.log.error('An error occurred!')
    ...
    >>> vbm_log.write_log()
    
  """
  # class Constructor
  def __init__(self, process_name, file_path, log_level='WARN', to_stdout=True, \
               storage_type=None, aws_access_key=None, aws_secret_key=None, aws_region=None, \
               azure_account=None, azure_credential=None, databricks_scope=None):
      
    # required
    self.process_name = process_name
    self.file_path = file_path
    
    # optional
    self.log_level = log_level
    self.to_stdout = to_stdout
    

    # instantiate sagacious.spark.IO class
    self.__io = IO(default_storage_type=storage_type, aws_access_key=aws_access_key, \
                   aws_secret_key=aws_secret_key, aws_region=aws_region, \
                   azure_account=azure_account , azure_credential=azure_credential, \
                   databricks_scope=databricks_scope)

    # Init python logging
    self.log = logging.getLogger(process_name)

    numeric_level = getattr(logging, self.log_level.upper().strip(), 10)
    self.log.setLevel(numeric_level)

    # Log to a byte stream 
    self.log_io = io.StringIO()

    file_hdlr = logging.StreamHandler(self.log_io)

    # Log format
    log_format = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    file_hdlr.setFormatter(log_format)
    file_hdlr.setLevel(self.log_level)
    self.log.addHandler(file_hdlr)

    # log also to stdout if requested
    if self.to_stdout:
      
      # if there is already an instance of a logging handler for console we should
      # remove this now
      for handler in self.log.handlers:
        if handler.stream.__class__.__name__ == 'ConsoleBuffer':
          self.log.removeHandler(handler)
    
      # define a Handler which writes messages to the sys.stderr
      console_hdlr = logging.StreamHandler(sys.stdout)

      console_hdlr.setFormatter(log_format)
      console_hdlr.setLevel(self.log_level)
      self.log.addHandler(console_hdlr)

    # suppress annoying py4j noisy stdout from console
    logging.getLogger('py4j').setLevel(logging.ERROR)


    # initialise the log file name
    datetime_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    self.log_file_name = os.path.join(self.file_path, self.process_name + '_' + datetime_str + '.log')

    #return cls._instance
  
  
  # writes log stream to file - either to ABS or S3
  def write_log(self):
    """Writes the content of the current log buffer to storage via the sagacious.utils.IO class.

    Returns:
      str: The file name where the log was written
    """
    try:
      
      from datetime import datetime
      import os
          
      # get log bytes
      log_bytes = self.log_io.getvalue().encode()
      
      self.__io.write_bytes(file_bytes=log_bytes, file_name=self.log_file_name)
      
      return self.log_file_name
    
    except Exception as e:
      raise Exception('Failed to write log file {0}\n{1}'.format(file_name, e))
      

  def get_log_buffer(self):
    """Returns log stream data as text string 
    
    Returns:
      str: The content of the current log buffer
    """
    
    return self.log_io.getvalue()
