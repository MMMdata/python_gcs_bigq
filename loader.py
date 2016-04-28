	
		 # Message describing how to use the script, when not in test mode
USAGE = """
Usage examples:
  $ python loader.py directory_name

"""
    
# get all configuration settings
import config



import logging
# create logger with 'spam_application'
logger = logging.getLogger('loader')
logger.filemode='w' # the log will be refreshed each time it is run
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh1 = logging.FileHandler('loader.log')
#fh.setLevel(logging.DEBUG)
fh1.setLevel(logging.INFO)
# create console handler with a higher log level
ch1 = logging.StreamHandler()
#ch.setLevel(logging.ERROR)
ch1.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh1.setFormatter(formatter)
ch1.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh1)
logger.addHandler(ch1)

        

import sys
import httplib2
# For even more detailed logging you can set the debug level of the httplib2 module used by this library. 
# The following code snippet enables logging of all HTTP request and response headers and bodies:
httplib2.debuglevel = 4
import os
import re
# used to capture stack traces
import traceback
from oauth2client.client import GoogleCredentials
# Discovery Service: Lets you discover information about other Google APIs, 
# such as what APIs are available, the resource and method details for each API    
from apiclient import discovery


             
def main(argv):

    try:
        
        schema_folder=config._SCHEMA_FOLDER
        logger.info("main - name of folder containing schema JSONs: %s\n", schema_folder)
        if os.path.exists(schema_folder):
                       
            ''' 
            Get the default credentials of this installed application, which, as per business requirements,
            these must be the credentials of a goodle service account.
            When running locally, default credentials are displayed after running the shell command `gcloud auth login`. 
            When running on google compute engine, default credentials are available from the environment.
            Default credentials can be programmatically set/configured in OS of the environment (see config file). 
            '''
            dflt_acct_creds=None
            dflt_acct_creds = GoogleCredentials.get_application_default() # this uses the credentials of an installed application
            if dflt_acct_creds:
                logger.info('main - have default credentials of installed application')
    
                '''
                Construct the service object for interacting with the Big Query API.
                '''
                dflt_acct_creds.user_agent = config._USER_AGENT,
                dflt_acct_creds.oauth_displayname = config._OAUTH_DISPLAY_NAME
        
                scoped_creds = dflt_acct_creds.create_scoped(config._BIG_QUERY_SCOPE)
                logger.info("main - set scope on default app credentials")              
                _AUTHORIZED_HTTP = scoped_creds.authorize(httplib2.Http())
                # Construct the service object for interacting with the BigQuery API.
                bq_client = discovery.build('bigquery', 'v2', http=_AUTHORIZED_HTTP)
                logger.info("main - have Google BigQuery service object client") 
    
                '''
                Construct the service object for interacting with the Cloud Storage API.
                '''
                gcs_client=None
                gcs_client = discovery.build('storage', 'v1', credentials=dflt_acct_creds)
                if gcs_client:
                    logger.info("main - have Google Cloud Storage service object client")                
                        
                    if os.path.exists(csv_folder):
                        for file_name in os.listdir(csv_folder):
                            logger.info("main - file_name: %s\n", file_name)
                            file_path_name=os.path.join(csv_folder, file_name)
                            logger.info("main - file_path_name: %s\n", str(file_path_name))
                            if os.path.isfile(file_path_name): # ignore folders so that convention violation errors are not thrown
                                entity_name=functions.EnforceFileNameConvention(file_name)
                                if entity_name:
                                    logger.info("main - entity_name: %s\n", entity_name)
                                    entity_schema=None
                                    entity_schema=functions.GetEntitySchema(schema_folder,entity_name)
                                    if entity_schema:
                                        logger.info('main - have JSON schema\n')
    
                                        if functions.isComplexLoad(entity_name):
                                            print 'main - COMPLEX PATH\n'     
                                            '''
                                            is this a monthly partition extract or a daily extract?
                                            '''
                                            '''
                                            monthly partition has _CCYYMMDD file name suffix
                                            '''
                                            search_result= re.search('_[0-9][0-9][0-9][0-9][0-9][0-9].csv', file_name, flags=0)
                                            if search_result:
                                                logger.info('main - initiate upload of monthly partition extract\n')
                                                '''
                                                pattern found in the file name (_CCYYMM.csv) indicates that a table partition extract 
                                                is to be uploaded from Google Cloud Storage into BigQuery
                                                only a baseline extract of a partition of a source table uses the _CCYYMM file name suffix
                                                therefore, TAKE THE SIMPLE PATH
                                                '''
                                                # stip off .csv suffix
                                                partition_name=file_name[:-4]
                                                logger.info('main - partition_name %s\n'%(partition_name))
                                                blob_name = file_name
                                                logger.info('main - file name: %s, mapped to blob name: %s\n'%(file_name,blob_name))
                                                bucket_name="wf_"+entity_name.lower()
                                                logger.info('main - file name: %s, mapped to gcs bucket name: %s\n'%(file_name,bucket_name)) 
                                                source_csv="gs://"+ bucket_name+"/"+ blob_name
                                                logger.info('main - source_csv %s\n'%(source_csv))
                                                logger.info('main - initiate upload of local CSV: %s into gcs bucket: %s\n'%(file_path_name, bucket_name)) 
                                                functions.LoadCsvIntoGcsBucket(file_path_name, gcs_client, file_name, entity_name)
                                                logger.info('main - initiate upload of CSV: %s from gcs bucket: %s, into Biq Query monthly partition table: %s\n'%(file_name, bucket_name, partition_name)) 
                                                functions.TruncateBulkLoadTable(bq_client, source_csv, entity_schema, partition_name)
                                                '''
                                                update the view over this table
                                                '''                                       
                                                view_ddl=complex_functions.CreateViewDDL(bq_client, entity_name)
                                                if view_ddl:
                                                    listofviewnames=[]
                                                    listofviewnames=complex_functions.GetListOfViewNames(bq_client)
                                                    for aviewname in listofviewnames:
                                                        if entity_name.upper() == aviewname.upper(): # filter by naming conventions
                                                            logger.info('main - initiate update of view query for table %s\n'%(entity_name))
                                                            complex_functions.UpdateView(bq_client, entity_name, entity_schema, view_ddl)

                                                if config._PURGE_GCS:
                                                    response=gcs_client.objects().delete(bucket=bucket_name, object=blob_name ).execute()
                                                    if 'error' in response:
                                                        logger.error('main - delete of blob %s from bucket %s failed.'%(blob_name,bucket_name))
                                                    else:
                                                        logger.info('main - deleted blob %s from bucket %s\n'%(blob_name,bucket_name))   

                                            else:
                                                '''
                                                this is a daily extract, TAKE THE COMPLEX PATH
                                                '''
                                                logger.info('main - initiate upload of daily extract file %s\n'%(file_path_name))
                                                complex_functions.ComplexLoad(gcs_client, bq_client, file_path_name, entity_name)

                                        else:
                                            print 'main - SIMPLE PATH\n'                                            
                                            blob_name = file_name
                                            logger.info('main - file name: %s, mapped to blob name: %s\n'%(file_name,blob_name))
                                            bucket_name="wf_"+entity_name.lower()
                                            logger.info('main - file name: %s, mapped to gcs bucket name: %s\n'%(file_name,bucket_name)) 
                                            source_csv="gs://"+ bucket_name+"/"+ blob_name
                                            logger.info('main - source_csv %s\n'%(source_csv)) 
                                            logger.info('main - initiate upload of local CSV: %s into gcs bucket: %s\n'%(file_path_name, bucket_name)) 
                                            functions.LoadCsvIntoGcsBucket(file_path_name, gcs_client, file_name, entity_name) 
                                            logger.info('main - initiate upload of CSV: %s from gcs bucket: %s, into Biq Query table: %s\n'%(file_name, bucket_name, entity_name)) 
                                            functions.TruncateBulkLoadTable(bq_client, source_csv, entity_schema, entity_name)
                                            if config._PURGE_GCS:
                                                response=gcs_client.objects().delete(bucket=bucket_name, object=blob_name ).execute()
                                                if 'error' in response:
                                                    logger.error('main - delete of blob %s from bucket %s failed.'%(blob_name,bucket_name))
                                                else:
                                                    logger.info('main - deleted blob %s from bucket %s\n'%(blob_name,bucket_name))   
                                    else:
                                        logger.error('main - failed to find JSON schema file that maps to CSV file\n')
                            else:
                                logger.info("main - file_path_name: %s, is not a file.\n", str(file_path_name))
                    else:
                        logger.error('main - csv_folder: %s, not found'%(csv_folder))
                else:
                    logger.error('main - failed to get google cloud storage client\n')
    
            else:
                logger.error('main - failed to get google service account credentials\n')
                
        else:
            logger.error('main - folder containing JSON schema files not found\n')

    except Exception, inst:
        logger.exception("main - Unexpected error: %s\n"%(str(sys.exc_info()[0])))
        if isinstance(inst,ValueError):
            logger.exception("main - ValueError: %s\n"%(inst.message))
        if isinstance(inst,AttributeError):
            logger.exception("main - AttributeError: %s\n"%(inst.message))
        if isinstance(inst,TypeError):
            logger.exception("main - TypeError: %s\n"%(inst.message))     
        logger.info('-'*60)
        logger.info(traceback.print_exc(file=sys.stdout))
        logger.info('-'*60)


if __name__ == '__main__':
    if not _TEST_MODE:
        if len(sys.argv) < 2:
            logger.critical('main - Too few arguments.\n')
            logger.info(USAGE)
        else:
            main(sys.argv)
    else:
        main(sys.argv)	 
