import json, socket
import asyncio, aiohttp
from config import libsConfig
from utils.log import get_logger
from libs.bigquery.async.auth import BigqueryToken

logger = get_logger('clients', 'bigquery')
libsConfig = libsConfig()


PROJECT_NAME = libsConfig['bigquery']['project_name']
CREDENTIALS_FILE = libsConfig['bigquery']['credential_file']

API_ROOT = 'https://www.googleapis.com/bigquery/v2'
QUERY_URL = API_ROOT + '/projects/'+ PROJECT_NAME +'/queries'
QUERY_RESULTS = API_ROOT + '/projects/'+ PROJECT_NAME +'/queries/{job_id}'

SCOPES = [
    'https://www.googleapis.com/auth/bigquery'
]

CONCURRENT_LIMIT = 200

def getTCPConnector():
    return aiohttp.TCPConnector(
                family=socket.AF_INET,
                verify_ssl=False,
                loop=asyncio.get_event_loop(),
                limit=CONCURRENT_LIMIT
                # limit_per_host=30
            )

class BigqueryAsync:

    def __init__(self):
        self.project = PROJECT_NAME
        self.session = None
        self.token = None

        """if self.session is None:
            aiohttpTCPConn = aiohttp.TCPConnector(
                family=socket.AF_INET,
                verify_ssl=False,
                loop=self.loop,
                limit=CONCURRENT_LIMIT
                # limit_per_host=30
            )
            self.session = aiohttp.ClientSession(connector=aiohttpTCPConn, loop=self.loop, connector_owner=False)"""

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.session.close()

    async def headers(self):
        # Error: aiohttp.client_exceptions.ClientConnectorError: Cannot connect to host www.googleapis.com:443 ssl:None [Device or resource busy]
        #if config.APP_ENV in ['local', 'dev']:

        if self.session is None:
            self.session = aiohttp.ClientSession()

        #async with sem:
        if self.token is None:
            self.token = BigqueryToken(
                PROJECT_NAME,
                CREDENTIALS_FILE,
                session=self.session,
                scopes=SCOPES
            )

        token = await self.token.get()

        return {
            'Authorization': f'Bearer {token}'
        }


    async def query(self, query):
        #await asyncio.sleep(2) # temp fix for avoiding the bigquery cuncurrent limit

        body = {
            'query': query,
            'useLegacySql': False
        }
        payload = json.dumps(body).encode('utf-8')

        headers = await self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json'
        })

        #No need of Semaphore, as aiohttp by default limits to 100 parallel connection
        #sem = asyncio.Semaphore(value=CONCURRENT_LIMIT)

        if self.session is None:
            self.session = aiohttp.ClientSession()

        try:
            async with self.session.post(QUERY_URL, data=payload, headers=headers, params=None, ssl=False, timeout=None) as response:
                content = await response.json()

                if 299 >= response.status >= 200:
                    if content['jobComplete']:
                        return {
                            'status': 'success',
                            'content': content
                        }
                    else:
                        return await self.getQueryJobResults(content['jobReference']['jobId'], self.session)
        except asyncio.TimeoutError:
            await asyncio.sleep(5)
            await asyncio.ensure_future( self.query(query) )
            #return {
            #    'status': 'error',
            #    'content': 'Asyncio timeout error'
            #}
        except Exception as e:
            return {
                'status': 'error',
                'content': str(e)
            }

        return {
            'status': 'error',
            'content': ''
        }


    async def getQueryJobResults(self, jobID, session):
        #await asyncio.sleep(5)

        headers = await self.headers()
        queryJobUrl = QUERY_RESULTS.format(job_id=jobID)

        try:
            async with session.get(queryJobUrl, headers=headers, ssl=False, timeout=None) as response:
                content = await response.json()
                if 299 >= response.status >= 200:
                    if not content['jobComplete']:
                        content = await self.getQueryJobResults(jobID, session)

                    #await session.close()
                    return {
                        'status': 'success',
                        'content': content
                    }
        except asyncio.TimeoutError:
            await asyncio.sleep(5)
            await asyncio.ensure_future( self.getQueryJobResults(jobID, session) )
        except Exception as e:
            #await session.close()
            return {
                'status': 'error',
                'content': str(e)
            }

        return {
            'status': 'error',
            'content': ''
        }


    @staticmethod
    def formatData(results):
        fields = results['schema']['fields']

        data = []
        for row in results['rows']:
            record = row['f']
            i = 0
            tmpData = {}
            for rec in record:
                tmpData[ fields[i]['name'] ] = rec['v']
                i += 1

            data.append(tmpData)

        return data