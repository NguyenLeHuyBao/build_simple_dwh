from config.dev import MysqlSource
from db_handler.mysql_handler import MysqlHandler
from db_handler.queries import MysqlStatements
import pandas as pd


def find_updated_records(mysql_handler, sql_query, list_columns):
    data = mysql_handler.action_db(query_str=sql_query, action='select')
    df = pd.DataFrame(data, columns=list_columns)
    df['location'] = 'VietNam'
    return df


def update_dwh_data(mysql_handler, data, table):
    dwh_handler = mysql_handler.dwh_object
    dwh_handler.action_db(action='update', data=data, table=table)


def check_update_mysql_source(**kwargs):
    mysql_handler = MysqlHandler(
        MysqlSource.HOST,
        MysqlSource.PORT,
        MysqlSource.USER,
        MysqlSource.PASSWORD,
        MysqlSource.DATABASE
    )

    postgres_table_statements = MysqlStatements()

    for table in postgres_table_statements.queries_object:
        print('Start checking for updated data from source Mysql - Table:', table['table'])
        data = find_updated_records(
            mysql_handler=mysql_handler,
            sql_query=table['check_update_statement'],
            list_columns=table['columns']
        )
        print('Found {0} updated records'.format(len(data)))

        print('Start updating DWH data - Table:', table['table'])

        update_dwh_data(
            mysql_handler=mysql_handler,
            data=data,
            table=table['table']
        )

        print('Finished update DWH data - Table:', table['table'])
