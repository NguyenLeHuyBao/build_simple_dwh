from config.dev import PostgresSource
from db_handler.postgres_handler import PostgresHandler
from db_handler.queries import PostgresStatements


import pandas as pd


def extract_data_from_source(postgres_handler, sql_query, list_columns):
    data = postgres_handler.action_db(query_str=sql_query, action='select')
    df = pd.DataFrame(data, columns=list_columns)
    df['location'] = 'ThaiLand'
    return df


def insert_data_to_dwh(postgres_handler, data, table):
    postgres_handler.action_db(data=data, table=table, action='push_data_to_dwh')


def ingest_psql_source(**kwargs):
    postgres_handler = PostgresHandler(
        PostgresSource.HOST,
        PostgresSource.PORT,
        PostgresSource.USER,
        PostgresSource.PASSWORD,
        PostgresSource.DATABASE
    )

    postgres_table_statements = PostgresStatements()

    for table in postgres_table_statements.queries_object:
        print('Start getting data from source Postgres - Table:', table['table'])
        data = extract_data_from_source(postgres_handler=postgres_handler,
                                        sql_query=table['select_statement'],
                                        list_columns=table['columns']
                                        )
        print('Found {0} records need to be inserted'.format(len(data)))

        print('Start ingesting data from source Postgres to DW - Table:', table['table'])

        insert_data_to_dwh(postgres_handler=postgres_handler, data=data, table=table['table'])

        print('Finished ingesting data from source to DW - Table:', table['table'])
