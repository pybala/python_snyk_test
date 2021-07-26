import pymssql
from utils.log import get_logger
from config import libsConfig

logger = get_logger('clients', 'sql')
libsConfig = libsConfig()


#ToDo: replace print with logs
class MssqlClient:

    __db_host = None
    __db_user = None
    __db_pass = None
    __db_name = None
    __db_conn = None
    __db_cursor = None

    def __init__(self):
        self.__db_host = libsConfig['sfdc_mssql']['db_ip']
        self.__db_user = libsConfig['sfdc_mssql']['db_username']
        self.__db_pass = libsConfig['sfdc_mssql']['db_password']
        self.__db_name = libsConfig['sfdc_mssql']['db_name']


    def __connect(self):
        try:
            self.__db_conn = pymssql.connect(self.__db_host, self.__db_user, self.__db_pass, self.__db_name)
            self.__db_cursor = self.__db_conn.cursor(as_dict=True)
        except pymssql.DatabaseError as e:
            logger.error("ODS connection error: %S", e)


    @property
    def db_client(self):
        if self.__db_conn is None:
            self.__connect()

        return self.__db_conn


    def execute(self, query):
        result = None
        if self.__db_conn is None:
            self.__connect()

        try:
            result = self.__db_cursor.execute(query)
        except pymssql.DatabaseError as e:
            print("Mssql Error: %S", e)

        self.close()
        return result


    def fetchone(self, query):
        results = None
        if self.__db_conn is None:
            self.__connect()

        try:
            self.__db_cursor.execute(query)
            results = self.__db_cursor.fetchone()
        except pymssql.DatabaseError as e:
            print("Mssql Error: %S", e)

        self.close()
        return results


    def fetchall(self, query):
        results = None
        if self.__db_conn is None:
            self.__connect()

        try:
            self.__db_cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
            self.__db_cursor.execute(query)
            results = self.__db_cursor.fetchall()
        except pymssql.DatabaseError as e:
            print("Mssql Error: %S", e)

        self.close()
        return results


    def close(self):
        if self.__db_cursor is not None:
            self.__db_cursor.close()
        if self.__db_conn is not None:
            self.__db_conn.close()

    def __del__(self):
        self.close()


if __name__ == "__main__":
    print('test')
