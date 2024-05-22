from config.dev import PostgresSource
from db_handler.postgres_handler import PostgresHandler
from db_handler.queries import PostgresStatements
import pandas as pd


def find_updated_records(postgres_handler, sql_query, list_columns):
    data = postgres_handler.action_db(query_str=sql_query, action='select')
    df = pd.DataFrame(data, columns=list_columns)
    df['location'] = 'ThaiLand'
    return df


def update_dwh_data(postgres_handler, data, table):
    dwh_handler = postgres_handler.dwh_object
    dwh_handler.action_db(action='update', data=data, table=table)


def check_update_psql_source(**kwargs):
    postgres_handler = PostgresHandler(
        PostgresSource.HOST,
        PostgresSource.PORT,
        PostgresSource.USER,
        PostgresSource.PASSWORD,
        PostgresSource.DATABASE
    )

    postgres_table_statements = PostgresStatements()

    for table in postgres_table_statements.queries_object:
        print('Start checking for updated data from source Postgres - Table:', table['table'])
        data = find_updated_records(
            postgres_handler=postgres_handler,
            sql_query=table['check_update_statement'],
            list_columns=table['columns']
        )
        print('Found {0} updated records'.format(len(data)))

        print('Start updating DWH data - Table:', table['table'])

        update_dwh_data(
            postgres_handler=postgres_handler,
            data=data,
            table=table['table']
        )

        print('Finished update DWH data - Table:', table['table'])
