import requests
import json
import pandas as pd
import os

from dotenv import load_dotenv

from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models.connection import Connection
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.sql import SqlSensor

@dag(
    start_date=datetime(2021, 1, 1),
    schedule=None,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30)
    }
)
def request_dag_func():
    start = EmptyOperator(task_id="start")

    @task()
    def get_response():
        load_dotenv()
        TOKEN_VAR = os.getenv('TOKEN_VAR')

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Token ' + TOKEN_VAR
        }
        url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/country"
        req = {
            'query': 'р'
        }

        data_request = requests.post(url, headers=headers, json=req)

        response_var = json.loads(data_request.text)

        return response_var
    
    @task()
    def get_values(response_arg: json):
        value_df = pd.DataFrame(columns=['value', 'unrestricted_value'])

        for i in range(0, len(response_arg['suggestions'])):
            temp_val = response_arg['suggestions'][i]['value']
            temp_unrestricted = response_arg['suggestions'][i]['unrestricted_value']

            temp_df = pd.DataFrame({"value": [temp_val], "unrestricted_value": [temp_unrestricted]})

            value_df = pd.concat([value_df, temp_df], ignore_index=True)

        return value_df

    @task()
    def change_letter(arg_df: pd.DataFrame):
        for index, row in arg_df.iterrows():
            if row['value'][0] == 'А':
                arg_df.at[index, 'value'] = arg_df.at[index, 'value'].replace('А', 'Н')
        return arg_df

    @task()
    def postgres_save(arg_df: pd.DataFrame):
        PG_CONN_ID = "postgres_conn" 
        postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        db_from_df = arg_df.to_sql(
            name='country',
            con=engine,
            schema='test',
            if_exists='replace',
            index=False
        )

        return 'country'
    
    @task()
    def get_car_response():
        load_dotenv()
        TOKEN_VAR = os.getenv('TOKEN_VAR')

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Token ' + TOKEN_VAR
        }
        url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/car_brand"
        req = {
            'query': 'форд'
        }

        data_request = requests.post(url, headers=headers, json=req)

        response_var = json.loads(data_request.text)

        return response_var
    
    @task()
    def get_car_values(response_arg: json):
        value_df = pd.DataFrame(columns=['value', 'unrestricted_value'])

        for i in range(0, len(response_arg['suggestions'])):
            temp_val = response_arg['suggestions'][i]['value']
            temp_unrestricted = response_arg['suggestions'][i]['unrestricted_value']

            temp_df = pd.DataFrame({"value": [temp_val], "unrestricted_value": [temp_unrestricted]})

            value_df = pd.concat([value_df, temp_df], ignore_index=True)

        return value_df

    @task()
    def postgres_car_save(arg_df: pd.DataFrame):
        PG_CONN_ID = "postgres_conn" 
        postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        db_from_df = arg_df.to_sql(
            name='car',
            con=engine,
            schema='test',
            if_exists='replace',
            index=False
        )

        return 'car'

    @task()
    def postgres_save_log(table_name: str):
        context = get_current_context()
        dag_name = context["dag"].dag_id

        log_df = pd.DataFrame({"table_nm": [table_name], "load_dttm": [datetime.today()], "status": ['Success'], "dag_nm": [dag_name]})

        PG_CONN_ID = "postgres_conn" 
        postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        db_from_df = log_df.to_sql(
            name='log_status',
            con=engine,
            schema='test',
            if_exists='append',
            index=False
        )

    country_finished_success = SqlSensor(
        task_id="country_finished_success",
        conn_id="postgres_conn",
        sql="""
            SELECT * FROM test.log_status 
            WHERE (
            table_nm = 'country' 
            AND status = 'Success' 
            AND load_dttm BETWEEN NOW() - INTERVAL '10 SECONDS' AND NOW()
            )""",
        poke_interval=2,
        timeout=60,
        mode='reschedule'
    )

    @task_group()
    def car_group():
        # country_finished_success >> postgres_save_log(postgres_car_save(get_car_values(get_car_response())))
        car_response_got = get_car_response()
        car_resp_df = get_car_values(car_response_got)
        final_op = postgres_car_save(car_resp_df)
        postgres_save_log(final_op)



    @task_group()
    def country_group():
        response_got = get_response()
        resp_df = get_values(response_got)
        new_resp_df = change_letter(resp_df)
        final_op = postgres_save(new_resp_df)
        postgres_save_log(final_op)
    

    end = EmptyOperator(task_id="end")

    start >> country_group() >> end
    start >> country_finished_success >> car_group() >> end

request_dag_func()


