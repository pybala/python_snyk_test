import motor.motor_asyncio
import asyncio
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
from utils.log import get_logger
from config import libsConfig

logger = get_logger('libs', 'MongoDB')
libsConfig = libsConfig()


"""
pymongo.errors.ConnectionFailure: 
    thrown whenever a connection to a database cannot be made (actually the super-class of AutoReconnect above)
pymongo.errors.ServerSelectionTimeoutError: 
    thrown whenever the query you issue cannot find a node to serve the request (e.g. issuing a read on primary, when no primary exists)
"""


#ToDo: indexing support
#ToDo: remove static methods, make instance with respective DBname as argument
class MongoAsync:
    __host = libsConfig['mongodb']['host']
    __port = libsConfig['mongodb']['port']
    __hosts_uri = libsConfig['mongodb']['hosts_uri']
    __replicaset = libsConfig['mongodb']['replica_set']
    __dbName = libsConfig['mongodb']['dbname']
    __client = None


    @staticmethod
    def connection_uri():
        """
        :return: Ex: mongodb://127.0.0.1:27017
        """
        connectionUri = 'mongodb://{}'.format(MongoAsync.__hosts_uri)
        if MongoAsync.__replicaset and MongoAsync.__replicaset != '':
            connectionUri = connectionUri + '/?replicaSet=' + MongoAsync.__replicaset

        return connectionUri


    @staticmethod
    def mongoClient(db_name=None):
        """
        Connecting to Client and DB doesnt require await
        :param db_name:
        :return:
        """
        if db_name is not None:
            MongoAsync.__dbName = db_name
        else:
            # switching to apps DB instead of default db
            MongoAsync.__dbName = libsConfig['mongodb']['dbname']

        # connect = motor.motor_asyncio.AsyncIOMotorClient(MongoAsync.__host, MongoAsync.__port)

        connectionUri = MongoAsync.connection_uri()
        connect = motor.motor_asyncio.AsyncIOMotorClient(connectionUri)

        return connect[MongoAsync.__dbName]


    @staticmethod
    async def findOne(colName, colDoc, db_name=None):
        """
        :param colName:
        :param colDoc:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[colName].find_one(colDoc)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: findMany : {}, {}".format(str(err), str(colDoc)))
            return None


    @staticmethod
    async def findMany(colName, colDoc, length=100, cursor=None, sortBy=None, db_name=None):
        """
        Default return records length: 100
        :param colName:
        :param colDoc:
        :param cursor:
        :param length:
        :param sortBy: {'field':'col name', 'direction': -1} OR list
        :param db_name:
        :return: Dict {'documents':'', 'cursor': cursor}
        """
        records = []
        recordsCount = None
        try:
            if cursor is None:
                cursor = MongoAsync.mongoClient(db_name)[colName].find(colDoc)

            try:
                recordsCount = await MongoAsync.mongoClient(db_name)[colName].count_documents(colDoc)
            except Exception:
                pass

            if sortBy is not None:
                if type(sortBy) != list:
                    sortDirection = sortBy['direction'] if 'direction' in sortBy else 1
                    cursor = cursor.sort(sortBy['field'], sortDirection)
                else:
                    cursor = cursor.sort(sortBy)

            for document in await cursor.to_list(length=length):
                records.append(document)

            return {
                'count': recordsCount,
                'documents': records,
                'cursor': cursor
            }
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: findMany : {}, {}".format(str(err), str(colDoc)))
            return None


    @staticmethod
    async def insertOne(colName, colDoc, db_name=None):
        """
        :param colName:
        :param colDoc:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[colName].insert_one(colDoc)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: insert record: {}, {}".format(str(err), str(colDoc)))
            return None


    @staticmethod
    async def insertMany(colName, colDocs, db_name=None):
        """
        :param colName:
        :param colDocs:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[colName].insert_many(colDocs)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: insert records: {}, {}".format(str(err), str(colDocs)))
            return None


    @staticmethod
    async def updateOne(col_name, find_doc, set_values, db_name=None):
        """
        #await coll.update_one({'i': 51}, {'$set': {'key': 'value'}})
        #update_one() only affects the first document it finds, you can update all of them with update_many()
        :param col_name:
        :param find_doc:
        :param set_values:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[col_name].update_one(find_doc, set_values)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: updating record: {}, {}".format(str(err), str(find_doc)))
            return None


    @staticmethod
    async def updateMany(col_name, find_doc, set_values, db_name=None):
        """
        #await coll.updateMany({'i': 51}, {'$set': {'key': 'value'}})
        :param col_name:
        :param find_doc:
        :param set_values:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[col_name].update_many(find_doc, set_values)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: updating records: {}, {}".format(str(err), str(find_doc)))
            return None


    @staticmethod
    async def deleteMany(colName, findDocs, db_name=None):
        """
        await coll.deleteMany({'i': 51}, {'$set': {'key': 'value'}})
        :param colName:
        :param colDocs:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[colName].delete_many(findDocs)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: deleting record: {}, {}".format(str(err), str(findDocs)))
            return None


    @staticmethod
    async def deleteOne(colName, findDoc, db_name=None):
        """
        :param colName:
        :param colDocs:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[colName].delete_one(findDoc)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: deleting records: {}, {}".format(str(err), str(findDoc)))
            return None


    @staticmethod
    async def dropCollection(colDoc, db_name=None):
        """
        :param colDoc:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name).drop_collection(colDoc)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: " + str(err))
            return str(err)

    @staticmethod
    async def count(colName, colDoc, db_name=None):
        """
        :param colName:
        :param colDoc:
        :param db_name:
        :return:
        """
        try:
            return await MongoAsync.mongoClient(db_name)[colName].count_documents(colDoc)
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError) as err:
            logger.error("MongoDB: " + str(err))
            return str(err)


async def mongoTest():
    """mc = MongoAsync.mongoClient()
    document = {'name': 'test'}
    #insert = await mc.test_collection.insert_one(document)
    #print(insert)

    document = await mc.my_collection.find_one({'name': 'bala'})
    print(document)

    document = await mc.person_account_evga.find_one({'evergage_id': '39c0c3ef1b623450'})
    print(document)

    insert = await MongoAsync.insertOne('test_collection', {'name': 'test'})
    print(insert)
    print(insert.inserted_id)

    #----------------------
    #start - Aggregate Lookup (join)
    colName = 'sa_slack_alert'
    lookup = [{
        '$lookup': {
            'from': 'sa_slack_action',
            'localField': 'slack_msg_ts',
            'foreignField': 'message_ts',
            'as': 'slack_actions'
        }
    }]
    cursor = MongoAsync.mongoClient()[colName].aggregate(lookup)
    async for doc in cursor:
        print(doc)
    #end - Aggregate Lookup (join)

    #----------------------
    colDoc = {}
    resp = await MongoAsync.findMany('sa_slack_action', colDoc, 2)
    for r in resp['documents']:
        print(r['_id'])

    resp = await MongoAsync.findMany('sa_slack_action', colDoc, 2, resp['cursor'])
    for r in resp['documents']:
        print(r['_id'])
    #----------------------
    colDoc = {
        'message_ts': '1554210778.000200'
    }
    sortBy = {
        'field': 'created_date',
        'direction': -1
    }
    resp = await MongoAsync.findMany('sa_slack_action', colDoc, 1, None, sortBy)
    print(resp)
    # ----------------------
    """

    insert = await MongoAsync.insertOne('test_collection', {'name': 'test'})
    print(insert)

    record_count = await MongoAsync.count('pendo_activeuse_push', {}, 'pendo')
    print(record_count)



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mongoTest())
    loop.close()
