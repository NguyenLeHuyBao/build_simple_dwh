from config.dev import DWHInfo
import psycopg2


class DwhHandler:
    def __init__(self):
        self.dwh_host = DWHInfo.HOST
        self.dwh_port = DWHInfo.PORT
        self.dwh_username = DWHInfo.USER
        self.dwh_password = DWHInfo.PASSWORD
        self.dwh_database = DWHInfo.DATABASE
        self.dwh_schema = 'raw_data'
        self.dwh_connection = self._create_dwh_connection()

    def _create_dwh_connection(self):
        conn = psycopg2.connect(
            database=self.dwh_database,
            user=self.dwh_username,
            password=self.dwh_password,
            host=self.dwh_host,
            port=self.dwh_port,
            options=f'-c search_path={self.dwh_schema}'
        )
        return conn

    def _get_data(self, query_str):
        cur = self.dwh_connection.cursor()
        cur.execute(query_str)
        records = cur.fetchall()
        return records

    def _push_data(self, data):
        pass

    def _change_data(self, data, table):
        new_value = ''
        cur = self.dwh_connection.cursor()
        for index, row in data.iterrows():
            for col in data.columns[1:]:
                temp = """{0} = '{1}', """.format(col, row[col])
                new_value += temp

            query_str = """
                    UPDATE raw_data.{table}
                    SET {new_value} updated_at = now() + INTERVAL '7 hours'
                    WHERE 1 = 1 
                        AND {search_key} = {search_value} 
                        AND location = '{location}'
                """.format(table=table,
                           new_value=new_value,
                           search_key=data.columns[0],
                           search_value=row[data.columns[0]],
                           location=row[data.columns[-1]])

            cur.execute(query_str)
            self.dwh_connection.commit()
        # self.dwh_connection.close()

    def action_db(self, query_str=None, action=None, data=None, table=None):
        """
        :param table: table in db to perform action
        :param query_str: string to query
        :param action: select or insert or None
        :param data: if action == Insert or action == Update then data is DataFrame, else None
        :return: result
        """
        if action == 'select':
            return self._get_data(query_str)
        elif action == 'insert':
            return self._push_data(query_str)
        else:
            return self._change_data(data, table)
