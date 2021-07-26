from utils.log import get_logger
from config import libsConfig

logger = get_logger('libs', 'MongoDB')
libsConfig = libsConfig()

class MongoSpark:
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
        connectionUri = 'mongodb://{}'.format(MongoSpark.__hosts_uri)
        if MongoSpark.__replicaset and MongoSpark.__replicaset != '':
            connectionUri = connectionUri + '/?replicaSet=' + MongoSpark.__replicaset

        return connectionUri

    @staticmethod
    def get_mongo_spark_options(db_name, colName):
        options = {
            "spark.mongodb.input.uri": MongoSpark.connection_uri(),
            "spark.mongodb.output.uri": MongoSpark.connection_uri(),
            "database": db_name,
            "collection": colName,
            "idAsObjectId": "false",
        }

        return options

    @staticmethod
    def save_to_mongo(df, options):
        try:
            df = df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("append") \
                .options(**options) \
                .save() 
        except Exception as err:
            logger.error('Error: Spark-Mongo: Dataframe save: %s', str(err))
