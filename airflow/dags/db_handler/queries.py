class PostgresStatements:
    def __init__(self):
        self.list_tables = ['users', 'orders', 'transactions', 'menus']
        self.list_columns = [
            ['user_id', 'name', 'email', 'mobile'],
            ['order_id', 'item_id', 'num', 'price'],
            ['user_id', 'order_id', 'paid_price', 'status'],
            ['item_id', 'name', 'price']
        ]

        self.queries_object = self._gen_queries()

    def _gen_queries(self):
        result = []
        info = {}
        for index, table in enumerate(self.list_tables):
            info['table'] = self.list_tables[index]
            info['columns'] = self.list_columns[index]
            info['select_statement'] = \
                'SELECT {0} FROM public.{1}'.format(', '.join(map(str, self.list_columns[index])),
                                                    self.list_tables[index])
            info['check_update_statement'] = \
                'SELECT {0} FROM {1} WHERE updated_at > created_at'.format(
                    ', '.join(map(str, self.list_columns[index])), self.list_tables[index])

            result.append(info)
            info = {}
        return result


class MysqlStatements:
    def __init__(self):
        self.list_tables = ['users', 'orders', 'transactions', 'menus']
        self.list_columns = [
            ['user_id', 'name', 'email', 'mobile'],
            ['order_id', 'item_id', 'num', 'price'],
            ['user_id', 'order_id', 'paid_price', 'status'],
            ['item_id', 'name', 'price']
        ]

        self.queries_object = self._gen_queries()

    def _gen_queries(self):
        result = []
        info = {}
        for index, table in enumerate(self.list_tables):
            info['table'] = self.list_tables[index]
            info['columns'] = self.list_columns[index]
            info['select_statement'] = \
                'SELECT {0} FROM report.{1}'.format(', '.join(map(str, self.list_columns[index])),
                                                    self.list_tables[index])
            info['check_update_statement'] = \
                'SELECT {0} FROM {1} WHERE updated_at > created_at'.format(
                    ', '.join(map(str, self.list_columns[index])), self.list_tables[index])

            result.append(info)
            info = {}
        return result


class DWHStatements:
    def __init__(self):
        self.list_tables = ['users', 'orders', 'transactions', 'menus']
        self.list_columns = [
            ['user_id', 'name', 'email', 'mobile'],
            ['order_id', 'item_id', 'num', 'price'],
            ['user_id', 'order_id', 'paid_price', 'status'],
            ['item_id', 'name', 'price']
        ]

        self.queries_object = self._gen_queries()

    def _gen_queries(self):
        result = []
        info = {}
        for index, table in enumerate(self.list_tables):
            info['table'] = self.list_tables[index]
            info['columns'] = self.list_columns[index]
            info['select_statement'] = \
                'SELECT {0} FROM raw_data.{1}'.format(', '.join(map(str, self.list_columns[index])),
                                                      self.list_tables[index])
            info['update_statment'] = ''
            result.append(info)
            info = {}
        return result
