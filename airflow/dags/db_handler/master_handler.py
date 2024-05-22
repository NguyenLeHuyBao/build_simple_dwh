import psycopg2
import psycopg2.extras as extras
from db_handler.dwh_handler import DwhHandler


class SourceMasterHandler:
    def __init__(self):
        self.source_connection = None
        self.dwh_object = DwhHandler()

    def initialize_source_connection(self, connection):
        self.source_connection = connection
        return connection

    def _get_data(self, query_str):
        cur = self.source_connection.cursor()
        cur.execute(query_str)
        records = cur.fetchall()
        return records

    def _push_data_to_dwh(self, data, table):
        tuples = [tuple(x) for x in data.to_numpy()]

        cols = ','.join(list(data.columns))

        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = self.dwh_object.dwh_connection.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            self.dwh_object.dwh_connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.dwh_object.dwh_connection.rollback()
            cursor.close()
            return 1
        print("the dataframe is inserted")
        cursor.close()

    def _change_data(self, query_str):
        cur = self.source_connection.cursor()
        cur.execute(query_str)
        print('Successfully modified data')

    def action_db(self, query_str=None, action=None, data=None, table=None):
        """
        :param table: table in db to perform action
        :param query_str: string to query
        :param action: select or insert or None
        :param data: if action == Insert then data is DataFrame, else None
        :return: result
        """
        if action == 'select':
            return self._get_data(query_str)
        elif action == 'push_data_to_dwh':
            return self._push_data_to_dwh(data, table)
        else:
            return self._change_data(query_str)


