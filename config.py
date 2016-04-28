<!DOCTYPE html>
_PROJECT_NUMBER="xxxxxxxxxxx"
_PROJECT_ID='xxxxxx-xxxxxx-xxxx'
_PROJECT_NAME=_PROJECT_ID

'''
only CSV files for which a matching JSON schema document exists
can be uploaded into a GCS bucket and on into a Big Query table
'''
_SCHEMA_FOLDER='./schema/' # contains the JSON files that define the Big Query table schema

'''
all source tables are classified as being either simple or complex
the names of all tables classified as requiring sharding are contained
within an array of the global variable _COMPLEX_PATH
'''
_COMPLEX_PATH=['TABLE_A','TABLE_B','TABLE_C','TABLE_D']

'''
determine if CSV files should be stored in Google Cloud Storage buckets
or purged after they are uploaded into a Big Query table
'''
_PURGE_GCS=False


import logging
# create logger with 'spam_application'
logger = logging.getLogger('config')
logger.filemode='w' # the log will be refreshed each time it is run
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh0 = logging.FileHandler('config.log')
#fh.setLevel(logging.DEBUG)
fh0.setLevel(logging.INFO)
# create console handler with a higher log level
ch0 = logging.StreamHandler()
#ch.setLevel(logging.ERROR)
ch0.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh0.setFormatter(formatter)
ch0.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh0)
logger.addHandler(ch0)


'''
All daily extract files of simple tables used to test the loader are
always, and only, stored within the _SIMPLE_PATH_TEST_DATA_FOLDER
located in a sub-folder to the loader.py root folder
'''
_SIMPLE_PATH_TEST_DATA_FOLDER='./testdata/simple/' # contains CSV files of 100s of simple source table daily extract test data

'''
to isolate business domain unit tests, daily extract unit test CSV files of complex tables must be segregated
into their own folder hierarchy

all monthly CSV extracts of a complex table are kept on-site, 
within the _MONTHLY_PARTITION_FOLDER partition folder, 
each complex table has its own exclusive sub-folder beneath _MONTHLY_PARTITION_FOLDER,
which is named, in lower case, after the name of the complex table.
by convention, the loader determines programatically at runtime which sub-folder to reference
based on the name of the complex table extract file it is handling.

'''
_MONTHLY_PARTITION_FOLDER='./testdata/complex/monthly/' # contains CSV files of monthly partitions of 4 complex source test data


'''
Variables used to intialize parameters to create 
an instance of the Big Query service client.

IGNORE DOCUMENTATION, this property must be a tuple with a trailing comma
'''   
_USER_AGENT = ('loader',)      
_OAUTH_DISPLAY_NAME = ('BQ file loader',)


# polling interval
_INTERVAL=5.0 # seconds
'''
GCS GLOBAL VARIABLES
'''

_FULLY_QUALIFIED_BUCKET_FOLDER='gs://cryptic_prefix_source/temp/'


'''
MANDATORY SETTING WHENEVER WORKING WITH CLIENT OF GOOGLE BIG QUERY
'''
_BIGQUERY_DATASET_ID=None
_BIG_QUERY_SCOPE='https://www.googleapis.com/auth/bigquery'

import os

'''
MANDATORY SETTINGS WHENEVER WORKING WITH GOOGLE SERVICE ACCOUNT
    AS PER BUSINESS REQUIREMENT
'''
_GServiceAccountPkey='./lib/gserviceaccountpkey.json'

#MANDATORY - set to GCP service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=_GServiceAccountPkey
logger.info("GOOGLE_APPLICATION_CREDENTIALS: %s"%(os.environ['GOOGLE_APPLICATION_CREDENTIALS']))

'''
MANDATORY SETTINGS WHENEVER WORKING WITH GOOGLE SDK
'''
_DATA_SET_ID=_PROJECT_NAME #must be set to literal data value of the project id
os.environ['GCLOUD_DATASET_ID']=_DATA_SET_ID  # MANDATORY SETTING
logger.info("GCLOUD_DATASET_ID: %s"%(os.environ['GCLOUD_DATASET_ID']))

'''
MANDATORY SETTINGS WHENEVER WORKING WITH API CLIENT
'''
_GAE_API_ROOT = 'https://www.'+_PROJECT_ID+'.appspot.com'
_GAE_API=None
_GAE_API_VERSION=None


_BIGQUERY_DATASET_ID='source'    

_GAE_NAME= _GAE_API_ROOT+'/'+_GAE_API+'/_ah/api'


'''
MANDATORY WHEN MOVING DATA SETS INTO GCP FROM ON-PREMISE
'''
import httplib2
# Retry transport and file IO errors.
_RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)
_NUM_RETRIES=5
_CHUNKSIZE=1024*1024


# Mimetype to use if one can't be guessed from the file extension.
_DEFAULT_MIMETYPE = 'application/octet-stream'
