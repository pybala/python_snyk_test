from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from app.utils import log
from app.config import config

LOG = log.get_logger()


PROJECT_NAME = config.get('bigquery', 'project_name')
CREDENTIALS_FILEPATH = config.get('bigquery', 'server_secret_file')
#CREDENTIALS_FILEPATH = config.get('pubsub', 'credentials_file')

"""
ToDo: APIBase with response variables
"""
class BigqueryConnect(object):

    _credentials = None
    _service = None

    def getClientService(self):
        if self._service is not None:
            pass

        self._service = bigquery.Client(project=PROJECT_NAME, credentials=self.__generate_credential())

        return self._service


    def __generate_credential(self):
        if self._credentials is not None:
            pass

        self.__credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILEPATH)

        return self.__credentials

