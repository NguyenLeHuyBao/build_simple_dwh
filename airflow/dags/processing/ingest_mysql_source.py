from config.dev import MysqlSource, DWHInfo
from db_handler.mysql_handler import MysqlHandler
from db_handler.queries import MysqlStatements

import psycopg2.extras as extras
import pandas as pd
import psycopg2


def extract_data_from_source(mysql_handler, sql_query, list_columns):
    data = mysql_handler.action_db(query_str=sql_query, action='select')
    df = pd.DataFrame(data, columns=list_columns)
    df['location'] = 'VietNam'
    return df


def insert_data_to_dwh(mysql_handler, data, table):
    mysql_handler.action_db(data=data, table=table, action='push_data_to_dwh')


def ingest_mysql_source(**kwargs):
    mysql_handler = MysqlHandler(
        MysqlSource.HOST,
        MysqlSource.PORT,
        MysqlSource.USER,
        MysqlSource.PASSWORD,
        MysqlSource.DATABASE
    )

    mysql_table_statements = MysqlStatements()

    for table in mysql_table_statements.queries_object:
        print('Start getting data from source Mysql - Table:', table['table'])
        data = extract_data_from_source(mysql_handler=mysql_handler,
                                        sql_query=table['select_statement'],
                                        list_columns=table['columns']
                                        )
        print('Found {0} records need to be inserted'.format(len(data)))

        print('Start ingesting data from source Mysql to DW - Table:', table['table'])

        insert_data_to_dwh(mysql_handler=mysql_handler, data=data, table=table['table'])

        print('Finished ingesting data from source to DW - Table:', table['table'])

    # for index, table in enumerate(MysqlStatements.list_tables):
    #     sql_query = MysqlStatements.list_queries[index]
    #     list_columns = MysqlStatements.list_columns[index]
    #
    #     print('Start getting data from source Mysql - Table:', table)
    #     data = mysql_handler.action_db(query_str=sql_query, action='select')
    #     print('Found {0} records need to be inserted'.format(len(data)))
    #     df = pd.DataFrame(data, columns=list_columns)
    #     df['location'] = 'VietNam'
    #
    #     print('Start ingesting data from source Mysql to DW - Table:', table)
    #     mysql_handler.action_db(action='insert', data=df, table=table)
    #     print('Finished ingesting data from source to DW - Table:', table)
