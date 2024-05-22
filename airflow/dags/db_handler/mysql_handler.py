import pymysql
from db_handler.master_handler import SourceMasterHandler


class MysqlHandler(SourceMasterHandler):
    def __init__(self, host, port, username, password, database):
        super().__init__()
        # Source config
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        self.connection = self._create_conn()
        self.initialize_source_connection(self.connection)

    def _create_conn(self):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
        )

        return conn
