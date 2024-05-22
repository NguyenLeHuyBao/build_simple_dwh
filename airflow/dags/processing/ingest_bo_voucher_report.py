from db_handler.postgres_handler import PostgresHandler
from config.dev import PostgresSource
from gspread_pandas import Spread, Client, conf
import pandas as pd
import os
from datetime import timedelta


def config_gg_sheet(spread_name=None, sheet_name=None):
    # path = '/Users/admin/Desktop/GalaxyPlay/bo_ingest_voucher_report/'
    path = os.path.abspath(os.getcwd())
    path = path + "/dags/config/"
    print('pathhh', path)
    spread_name = 'GP Voucher Report' if not spread_name else spread_name
    sheet_name = 'Total' if not sheet_name else sheet_name
    c = conf.get_config(path, "datateam-credentials.json")
    spread = Spread(spread=spread_name, sheet=sheet_name, config=c)

    return spread


def create_data_final(data_sheet, columns):
    year_prefix = '2023'
    data_final = []
    for column in data_sheet.columns:
        year_month = column.split(' ')[1].zfill(2)
        month_key = year_prefix + year_month

        for date, value in data_sheet[column].items():
            date_key = year_prefix + year_month + date.zfill(2)
            date_string = date_key
            group_by_dimension = 'total_sold'
            period = 'daily'
            values_1 = value
            values_2 = 0

            data_final.append({
                'date_key': date_key,
                'date_string': date_string,
                'month_key': month_key,
                'group_by_dimension': group_by_dimension,
                'period': period,
                'values_1': values_1,
                'values_2': values_2
            })

    data_df = pd.DataFrame(data_final)[columns]
    return data_df


def data_preprocessing(spread, execution_date=None, day_before=None):
    data_sheet = spread.sheet_to_df(index=2, start_row=4)
    data_sheet = data_sheet.iloc[:, 1:-1].iloc[:-3]
    data_sheet = data_sheet.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    data_sheet = data_sheet.applymap(lambda x: '0' if x in ['-', '', '#VALUE!'] else x)
    data_sheet = data_sheet.applymap(lambda x: x.replace('.', '') if isinstance(x, str) and '.' in x else x)
    data_sheet = data_sheet.apply(pd.to_numeric, errors='coerce')
    return data_sheet


def insert_data_to_dwh(postgres_handler, data, table):
    postgres_handler.action_db(data=data, table=table, action='push_data_to_dwh')


def ingest_bo_voucher_report(**kwargs):
    execution_time = kwargs["execution_date"]
    next_execution_time = kwargs['next_execution_date']
    execution_date = execution_time.strftime("%Y-%m-%d")

    day_before_execution_time = execution_time - timedelta(days=1)
    day_before = day_before_execution_time.strftime("%Y-%m-%d")
    next_execution_day = next_execution_time.strftime("%Y-%m-%d")
    postgres_handler = PostgresHandler(
        PostgresSource.HOST,
        PostgresSource.PORT,
        PostgresSource.USER,
        PostgresSource.PASSWORD,
        PostgresSource.DATABASE
    )

    print('execution_date:', execution_date)
    print('day_before:', day_before)
    print('next_execution_day', next_execution_day)

    spread = config_gg_sheet()

    data_sheet = data_preprocessing(spread, execution_date=execution_date, day_before=day_before)

    columns = ['date_key', 'date_string', 'month_key', 'group_by_dimension', 'period', 'values_1', 'values_2']

    data_final = create_data_final(data_sheet, columns)

    insert_data_to_dwh(postgres_handler=postgres_handler, data=data_final, table='raw_data.partnership_cinema_daily')
