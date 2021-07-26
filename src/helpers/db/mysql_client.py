import pymysql

from config import libsConfig
from utils.log import get_logger

libsConfig = libsConfig()
logger = get_logger('clients', 'mysql_util')


class Mysqldb:

	__db_host = None
	__db_user = None
	__db_pass = None
	__db_name = None
	__db_conn = None
	__db_cursor = None

	def __init__(self, db_obj=None):
		self.__db_host = libsConfig['mysql_db']['HOST']
		self.__db_user = libsConfig['mysql_db']['USER']
		self.__db_pass = libsConfig['mysql_db']['PASSWORD']
		self.__db_name = libsConfig['mysql_db']['DBNAME']

		if db_obj is not None:
			self.__db_host = db_obj['HOST']
			self.__db_user = db_obj['USER']
			self.__db_pass = db_obj['PASSWORD']
			self.__db_name = db_obj['DBNAME']

	def __connect(self):
		try:
			self.__db_conn = pymysql.connect(self.__db_host, self.__db_user, self.__db_pass, self.__db_name)
			self.__db_cursor = self.__db_conn.cursor(pymysql.cursors.DictCursor)
		except Exception as err:
			logger.error(err)

	def execute(self, query):
		result = None
		if self.__db_conn is None:
			self.__connect()

		try:
			result = self.__db_cursor.execute(query)
			self.__db_conn.commit()
		except Exception as err:
			logger.error(err)

		#self.close()
		return result

	def insert_row(self, query):
		if self.__db_conn is None:
			self.__connect()
		result = None
		try:
			result = self.__db_cursor.execute(query)
			self.__db_conn.commit()
			logger.info("Row inserted successfully")
		except Exception as err:
			logger.error(err)
		return result
		# self.close()

	def insert_rows(self, query):
		if self.__db_conn is None:
			self.__connect()

		try:
			self.__db_cursor.executemany(query)
			self.__db_conn.commit()
			logger.info("Row inserted successfully")
		except Exception as err:
			logger.error(err)

		# self.close()

	def delete_row(self, query):
		if self.__db_conn is None:
			self.__connect()

		try:
			self.__db_cursor.execute(query)
			self.__db_conn.commit()
			logger.info("Row deleted successfully")
		except Exception as err:
			logger.error(err)

		# self.close()

	def delete_rows(self, query):
		if self.__db_conn is None:
			self.__connect()

		try:
			self.__db_cursor.execute(query)
			self.__db_conn.commit()
			logger.info("Rows deleted successfully")
		except Exception as err:
			logger.error(err)

		# self.close()

	def fetch_one(self, query):
		result = None
		if self.__db_conn is None:
			self.__connect()

		try:
			self.__db_cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
			self.__db_cursor.execute(query)
			result = self.__db_cursor.fetchone()
		except Exception as err:
			logger.error(err)

		# self.close()
		return result

	def fetch_all(self, query):
		results = None
		if self.__db_conn is None:
			self.__connect()

		try:
			self.__db_cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
			self.__db_cursor.execute(query)
			results = self.__db_cursor.fetchall()
		except Exception as err:
			logger.error(err)

		# self.close()
		return results

	def close(self):
		self.__db_cursor.close()
		self.__db_conn.close()

	def check_db_exists(self, db_name):
		try:
			db_conn = pymysql.connect(self.__db_host, self.__db_user, self.__db_pass)
			db_cursor = db_conn.cursor(pymysql.cursors.DictCursor)

			db_cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
			db_cursor.execute("SHOW DATABASES")
			dbs = db_cursor.fetchall()

			db_exist_flag = False
			for db in dbs:
				if db['Database'] == db_name:
					db_exist_flag = True
					break

			db_cursor.close()
			db_conn.close()
			return db_exist_flag
		except Exception as err:
			logger.error(err)

	def create_db(self, db_name):
		try:
			db_conn = pymysql.connect(self.__db_host, self.__db_user, self.__db_pass)
			db_cursor = db_conn.cursor(pymysql.cursors.DictCursor)

			# SQL Statement to create a database
			sql_statement = "CREATE DATABASE " + db_name

			# Execute the create database SQL statement through the cursor instance
			db_cursor.execute(sql_statement)
			logger.info("Database created successfully - " + db_name)
			db_cursor.close()
			db_conn.close()
		except Exception as err:
			logger.error(err)
