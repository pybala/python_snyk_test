import re, datetime
from googleapiclient.errors import HttpError
from app.utils import log
from app.config import config
from app.libs.bq.bigquery_connect import BigqueryConnect

LOG = log.get_logger()

PROJECT_NAME = config.get('bigquery', 'project_name')
STORAGE_ROOT = config.get('bigquery', 'storage_root')

class BigqueryJob(BigqueryConnect):

    _client = None

    def __init__(self):
        self._client = self.getClientService()


    def getJobState(self, jobId):
        jobList = self._client.list_jobs()

        for job in jobList:  # API request(s)
            if job.name == jobId:
                return job.state

        return False


    def loadData(self, dsName, tableName, sql):
        dataset = self._client.dataset(dsName)
        table = dataset.table(name=tableName)
        jobID = dsName + '_' + tableName + "_insert_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        query = self._client.run_async_query(jobID, sql)
        #query.use_legacy_sql = use_legacy_sql
        #query.use_query_cache = use_query_cache
        #query.maxResults = max_results
        query.destination = table
        query.write_disposition = "WRITE_TRUNCATE"
        query.begin(self._client)

        return jobID


    def exportData(self, dsName, tableName, viewName):
        dataset = self._client.dataset(dsName)
        table = dataset.table(name=tableName)
        jobID = dsName + '-' + tableName + "-export-" + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        fileName = STORAGE_ROOT + "/" + dsName + "/"+ viewName +".csv"

        exportJob = self._client.extract_table_to_storage(jobID, table, fileName)
        exportJob.destination_format = 'CSV'
        exportJob.print_header = True
        exportJob.write_disposition = 'truncate'
        exportJob.begin(self._client)

        return jobID
