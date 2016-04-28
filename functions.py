
		 
import config 

import logging
# create logger with 'spam_application'
logger = logging.getLogger('test_functions')
logger.filemode='w' # the log will be refreshed each time it is run
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh2 = logging.FileHandler('test_functions.log')
#fh.setLevel(logging.DEBUG)
fh2.setLevel(logging.INFO)
# create console handler with a higher log level
ch2 = logging.StreamHandler()
#ch.setLevel(logging.ERROR)
ch2.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh2.setFormatter(formatter)
ch2.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh2)
logger.addHandler(ch2)



import json
# format HTTP responses so that they are consumable by mere mortals
from json import dumps as json_dumps
import uuid
import httplib2
# For even more detailed logging you can set the debug level of the httplib2 module used by this library. 
# The following code snippet enables logging of all HTTP request and response headers and bodies:
httplib2.debuglevel = 4
# used to capture stack traces
import traceback
import sys
import random
import time
import re
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload

    
# GOOGLE CODE
def handle_progressless_iter(error, progressless_iters):
    if progressless_iters > config._NUM_RETRIES:
        logger.info('Failed to make progress for too many consecutive iterations.')
        raise error

    sleeptime = random.random() * (2**progressless_iters)
    logger.info('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
        % (str(error), sleeptime, progressless_iters))
    time.sleep(sleeptime)
# end of GOOGLE CODE


'''
isNotEmpty("")    returns False
isNotEmpty("   ") returns False
isNotEmpty("ok")  returns True
isNotEmpty(None)  returns False
'''
def isNotEmpty(s):
    return bool(s and s.strip())



def isComplexLoad(entity_name):
    try:
        if entity_name.upper() in config._COMPLEX_PATH:
            return True
        else:
            return False
    except Exception, ex:
        logger.error(json_dumps(ex, indent=2))  


def EnforceFileNameConvention(file_name):
    try:
        if not isNotEmpty(file_name):   
            logger.info('EnforceFileNameConvention - file_name is empty') 
            return None
        '''
        Enforce CSV file naming convention:
            TableName_CCYYMM for a table partition extract, and
            TableName_CCYYMMDD for a daily extract of a source table.
        
        An extract from a source table is never more than a snapshot at a point in time.
        
        '''
        logger.info('EnforceFileNameConvention - file name %s\n'%(file_name))
        # stip off .csv suffix
        partition_name=file_name[:-4]
        logger.info('EnforceFileNameConvention - partition_name %s\n'%(partition_name))
        
        enforcesconvention=False
        entity_name=None
         
        '''
        _CCYYMMDD file name suffix
        '''
        search_result= re.search('_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].csv', file_name, flags=0)
        if search_result:
            logger.info('EnforceFileNameConvention - pattern _CCYYMMDD')
            '''
            pattern found in the file name indicates that a daily extract of a table _CCYYMMDD
            is to be uploaded from Google Cloud Storage into BigQuery
            both simple and complex daily extracts use the _CCYYMMDD file name suffix
            '''
            enforcesconvention=True
            entity_name=partition_name[:-9]
            
        '''
        _CCYYMM file name suffix - used only by baseline table partition extract files
        '''
        search_result= re.search('_[0-9][0-9][0-9][0-9][0-9][0-9].csv', file_name, flags=0)
        if search_result:
            logger.info('EnforceFileNameConvention - pattern _CCYYMM')
            '''
            pattern found in the file name indicates that the a table partition extract _CCYYMM
            is to be uploaded from Google Cloud Storage into BigQuery
            only a baseline extract of a partition of a source table uses the _CCYYMM file name suffix
            '''
            enforcesconvention=True
            entity_name=partition_name[:-7]
            
            
        if not enforcesconvention:
            '''
            the file name violates the file naming convention
            '''
            logger.error('EnforceFileNameConvention - file name %s violates convention, it will not be uploaded into Google Cloud Storage'%(partition_name))
        
        return entity_name
    except Exception, ex:
        logger.exception(ex)    
        logger.exception('-'*60)
        logger.exception(traceback.print_exc(file=sys.stdout))
        logger.exception('-'*60)
        return None 


def LoadCsvIntoGcsBucket(file_path_name, gcs_client, file_name, entity_name):
    try:
        if not isNotEmpty(file_path_name):   
            logger.info('LoadCsvIntoGcsBucket - file_path_name is empty') 
            return None
        if not gcs_client:   
            logger.info('LoadCsvIntoGcsBucket - gcs_client is empty') 
            return None
        if not isNotEmpty(file_name):   
            logger.info('LoadCsvIntoGcsBucket - file_name is empty') 
            return None
        if not isNotEmpty(entity_name):  
            logger.info('LoadCsvIntoGcsBucket - entity_name is empty') 
            return None     
                
        logger.info('LoadCsvIntoGcsBucket - file_path_name name: %s\n'%(str(file_path_name)))
        blob_name = file_name
        logger.info('LoadCsvIntoGcsBucket - file name: %s, mapped to blob name: %s\n'%(file_name,blob_name))
        bucket_name="wf_"+entity_name.lower()
        logger.info('LoadCsvIntoGcsBucket - file name: %s, mapped to gcs bucket name: %s\n'%(file_name,bucket_name))                            
        '''
        determine if bucket exists
        '''
        bucket_exists=False
        fields_to_return = 'nextPageToken,items(name,location,timeCreated)'
        req = gcs_client.buckets().list(
                project=config._PROJECT_ID,
                fields=fields_to_return,  # optional
                maxResults=42)            # optional
        
        '''
        If you have too many items to list in one request, list_next() will
        automatically handle paging with the pageToken. 
        The list is limited to 52 file, per request
        '''
        while req is not None:
            resp = req.execute()
            items = resp['items']
            for item in items:
                if item["name"] == bucket_name:
                    bucket_exists=True
            req = gcs_client.buckets().list_next(req, resp)
                   
        if bucket_exists:
            logger.info('LoadCsvIntoGcsBucket - bucket: %s, exists\n'%(bucket_name))
            if _TEST:
                print('LoadCsvIntoGcsBucket - file name: %s, mapped to gcs bucket name: %s\n'%(file_name,bucket_name)) 
                
            '''
            If file already exists in bucket, then delete the file.
            Create a request to objects.list to retrieve a list of objects inside the bucket.
            
            '''
            fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
            req = gcs_client.objects().list(bucket=bucket_name, fields=fields_to_return)
        
            while req is not None:
                resp = req.execute()
                #logger.info( '\n\n resp: %s\n'%(resp))
                if resp:
                    if resp['items']:
                        items=resp['items']
                        for item in items:
                            if item["name"] == blob_name:
                                logger.info('LoadCsvIntoGcsBucket - blob: %s found in bucket %s\n'%(blob_name,bucket_name))
                                # delete existing file
                                response=gcs_client.objects().delete(bucket=bucket_name, object=blob_name ).execute()
                                if 'error' in response:
                                    logger.error('LoadCsvIntoGcsBucket - delete of blob %s from bucket %s failed.'%(blob_name,bucket_name))
                                else:
                                    logger.info('LoadCsvIntoGcsBucket - deleted blob %s from bucket %s\n'%(blob_name,bucket_name))   
                            #else:
                            #    logger.info("LoadCsvIntoGcsBucket - bucket %s is empty\n'%(bucket_name))
                
                req = gcs_client.objects().list_next(req, resp)
                                    
            logger.info('LoadCsvIntoGcsBucket - building upload CSV request to GCS ...')
            media = MediaFileUpload(file_path_name, chunksize=config._CHUNKSIZE, resumable=True)
            if not media.mimetype():
                media = MediaFileUpload(file_path_name, config._DEFAULT_MIMETYPE, resumable=True)
            request = gcs_client.objects().insert(bucket=bucket_name, name=blob_name,
                                               media_body=media)
            
            logger.info('LoadCsvIntoGcsBucket - uploading CSV file: %s, to bucket: %s, as blob: %s\n' % (file_path_name, bucket_name,
                                                                    blob_name))
            
            progressless_iters = 0
            response = None
            while response is None:
                error = None
                try:
                    progress, response = request.next_chunk()
                    if progress:
                        logger.info('LoadCsvIntoGcsBucket - upload %d%%' % (100 * progress.progress()))
                except HttpError, err:
                    error = err
                    if err.resp.status < 500:
                        raise
                except logger._RETRYABLE_ERRORS, err:
                    error = err
            
                if error:
                    progressless_iters += 1
                    handle_progressless_iter(error, progressless_iters)
                else:
                    progressless_iters = 0
            
            logger.info('LoadCsvIntoGcsBucket - upload complete!\n')
            logger.info(json_dumps(response, indent=2)+'\n')
            return True
        else:
            logger.error('LoadCsvIntoGcsBucket - bucket: %s, does not exist\n'%(bucket_name))
            return False
    except HttpError as err:
        logger.error(json_dumps(err, indent=2))
        return False
    except Exception,ex:
        logger.exception(ex)    
        logger.exception('-'*60)
        logger.exception(traceback.print_exc(file=sys.stdout))
        logger.exception('-'*60)
        return False
                                    
                                        
def GetEntitySchema(schema_folder,entity_name):
    try:
        if not isNotEmpty(schema_folder):   
            logger.info('GetEntitySchema - schema_folder is empty') 
            return None
        if not isNotEmpty(entity_name):  
            logger.info('GetEntitySchema - entity_name is empty') 
            return None     
                
        logger.info('GetEntitySchema get JSON file for: %s\n'%(entity_name))
        entity_schema=None
        import os
        files = os.listdir(schema_folder) 
        for json_file in files:
            # stip off .json suffix
            named_schema_json=json_file[:-5]
            #logger.info('GetEntitySchema found JSON file: %s\n'%(named_schema_json))
            if entity_name.lower()==named_schema_json.lower():
                logger.info('GetEntitySchema - found JSON file containing Big Query table definition')
                '''
                read contents of JSON file containing Big Query table definition
                '''
                with open(schema_folder+'/'+json_file) as opened_json_file:
                    entity_schema=json.load(opened_json_file)
                if _TEST:
                    from pprint import pprint
                    pprint(entity_schema)
        return entity_schema

    except HttpError as err:
        logger.error(json_dumps(err, indent=2))
        return False
    except Exception,ex:
        logger.exception(ex)    
        logger.exception('-'*60)
        logger.exception(traceback.print_exc(file=sys.stdout))
        logger.exception('-'*60)
        return False



# [START poll_job]
def poll_job(bq_client, projectId, jobId, interval=5.0, num_retries=5):
    """checks the status of a job every *interval* seconds"""
    logger.info('poll_job - enter, projectId: %s, jobId: %s\n'%(str(projectId), str(jobId))) 

    job_get = bq_client.jobs().get(projectId=projectId, jobId=jobId)
    job_resource = job_get.execute(num_retries=num_retries)

    while not job_resource['status']['state'] == 'DONE':
        logger.info('poll_job - Job is {}, waiting {} seconds...'
              .format(job_resource['status']['state'], interval))
        time.sleep(float(interval))
        job_resource = job_get.execute(num_retries=num_retries)

    return job_resource
# [END poll_job]


# [START load_table]
def load_table(bq_client, entity_schema, source_csv, entity_name):
    logger.info('load_table - enter, source_csv: %s, entity_name: %s\n'%(source_csv, entity_name)) 

    # Generate a unique job_id so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': config._PROJECT_ID,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'load': {
                'sourceUris': [source_csv],
                'schema': {
                    'fields': entity_schema
                },
                'destinationTable': {
                    'projectId': config._PROJECT_ID,
                    'datasetId': config._BIGQUERY_DATASET_ID,
                    'tableId': entity_name.upper() # enforce naming convention
                },
            'createDisposition': 'CREATE_IF_NEEDED', 
            'writeDisposition': 'WRITE_TRUNCATE',
            'fieldDelimiter': '|',
            'skipLeadingRows': 1,
            'encoding': 'UTF-8',
            'maxBadRecords': 100,
            'allowQuotedNewlines': True,
            'sourceFormat': 'CSV',
            'allowJaggedRows': True,
            'ignoreUnknownValues': True
            }
        }
    }                
    logger.info('load_table - job_data:\n %s\n'%(json_dumps(unicode(job_data), indent=2)))

    return bq_client.jobs().insert(
        projectId=config._PROJECT_ID,
        body=job_data).execute(num_retries=config._NUM_RETRIES)
# [END load_table]




# [START run]


def run(bq_client, entity_schema, source_csv, entity_name):
    logger.info('run - enter\n') 
    job = load_table(bq_client, entity_schema, source_csv, entity_name.upper())

    poll_job(bq_client,
             job['jobReference']['projectId'],
             job['jobReference']['jobId'],
             config._INTERVAL,
             config._NUM_RETRIES)
    logger.info('run - exit\n') 
# [END run]

        
      
def TruncateBulkLoadTable(bq_client, source_csv, entity_schema, entity_name):
    try:
        if not bq_client:   
            logger.info('TruncateBulkLoadTable - bq_client is empty') 
            return None
        if not isNotEmpty(source_csv):   
            logger.info('TruncateBulkLoadTable - source_csv is empty') 
            return None
        if not entity_schema:   
            logger.info('TruncateBulkLoadTable - entity_schema is empty') 
            return None
        if not isNotEmpty(entity_name):  
            logger.info('TruncateBulkLoadTable - entity_name is empty') 
            return None     


        '''
        extracted files are delimited using | and have a header in line #1
        Generate a unique job_id so retries don't accidentally duplicate query
        '''                
        logger.info('TruncateBulkLoadTable - Begin load of CSV %s into BQ table %s\n'%(source_csv,entity_name))
        
        job_data = {
            'jobReference': {
                'projectId': config._PROJECT_ID,
                'job_id': str(uuid.uuid4())
            },
            'configuration': {
                'load': {
                    'sourceUris': [source_csv],
                    'schema': {
                        'fields': entity_schema
                    },
                    'destinationTable': {
                        'projectId': config._PROJECT_ID,
                        'datasetId': config._BIGQUERY_DATASET_ID,
                        'tableId': entity_name.upper() # enforce naming convention
                    },
                'createDisposition': 'CREATE_IF_NEEDED', 
                'writeDisposition': 'WRITE_TRUNCATE',
                'fieldDelimiter': '|',
                'skipLeadingRows': 1,
                'encoding': 'UTF-8',
                'maxBadRecords': 100,
                'allowQuotedNewlines': True,
                'sourceFormat': 'CSV',
                'allowJaggedRows': True,
                'ignoreUnknownValues': True
                }
            }
        }

        
        request = bq_client.jobs().insert(projectId=config._PROJECT_ID,
            body=job_data).execute(num_retries=config._NUM_RETRIES)
                                
        logger.info('TruncateBulkLoadTable - Uploading file: %s, into Big Query table: %s ' % (source_csv, entity_name.upper()))
 
        return run(bq_client,
            entity_schema,
            source_csv, # a.k.a. data_file_path
            entity_name.upper()) # enforce naming convention
     
        logger.info('TruncateBulkLoadTable - Upload complete!') #

    except HttpError as err:
        logger.error(json_dumps(err, indent=2))
        return False
    except Exception,ex:
        logger.exception(ex)    
        logger.exception('-'*60)
        logger.exception(traceback.print_exc(file=sys.stdout))
        logger.exception('-'*60)
        return False

