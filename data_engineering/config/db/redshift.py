import configparser
import psycopg2


class RedshiftConnection:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('../../config.ini')

        self._user = config['REDSHIFT']['USER']
        self._password = config['REDSHIFT']['PASSWORD']
        self._host = config['REDSHIFT']['HOST']
        self._port = config['REDSHIFT']['PORT']
        self._database = config['REDSHIFT']['DATABASE']
        self._db_connection = psycopg2.connect(
            "dbname={dbname} user={user} host={host} password={password} port={port}".format(
                dbname=self._database,
                user=self._user,
                password=self._password,
                host=self._host,
                port=self._port
            ))
        self._db_connection.set_session(autocommit=True)
        self._cursor = self._db_connection.cursor()

    def query(self, query):
        return self._cursor.execute(query)

    def queryWithParam(self, query, params):
        return self._cursor.execute(query, params)

    def __del__(self):
        self._db_connection.close()
