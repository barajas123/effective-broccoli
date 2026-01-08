from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import json
import requests
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import timedelta

# Enable logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# default args
default_args = {
    'start_date': days_ago(1),
    'email': ['andy@brjs.com'],  # Email to notify
    'email_on_failure': True,  # Notify when any task fails
    'email_on_retry': False,   # Optional: Notify on retries
    'retries': 0,  # Number of retries
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Function that raises an exception
def fail_task():
    raise Exception("Intentional failure for testing email notifications.")

force_fail = PythonOperator(
    task_id='force_failure',
    python_callable=fail_task
)


# DAG definition
with DAG(
    dag_id='mongodb_api_post_and_get_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['mongodb', 'http'],
) as dag:

    # Task 1: Pull stock symbols from MongoDB API
    def parse_symbols_from_mongodb(ti):
        response = ti.xcom_pull(task_ids='post_to_mongodb')
        response_json = json.loads(response)
        symbols = [doc['symbol'] for doc in response_json.get('documents', [])]
        log.info(f"Symbols extracted: {symbols}")
        ti.xcom_push(key='symbols', value=symbols)

    # Task to call MongoDB API
    post_to_mongodb = SimpleHttpOperator(
        task_id='post_to_mongodb',
        http_conn_id='mongo_default',
        endpoint='/app/data-kauzd/endpoint/data/v1/action/find',
        method='POST',
        headers={
            "Content-Type": "application/json",
            "api-key": "GgPGg4qpFg5tyqIB5u593WUOixvX62Hk09M0XKnhUpFrnXIcx0JGtbnuuWOWchd7"
        },
        data='''
        {
           "collection":"symbol",
           "database":"brjs-io-db-symbol",
           "dataSource":"brjs-io-db-symbol",
           "filter" : {}
        }
        ''',
        log_response=True,
        do_xcom_push=True
    )

    # Task 2: Parse symbols from the MongoDB response
    parse_symbols = PythonOperator(
        task_id='parse_symbols',
        python_callable=parse_symbols_from_mongodb
    )
  
    def fetch_symbol_data(symbol, server_url, h):
        url = f'{server_url}/{symbol}?month=ALL'
        try:
            log.info(f"Fetching data for symbol: {symbol}")
            response = requests.get(url, headers=h)
            if response.status_code == 200:
                data = response.json()
                data['date'] =  str(datetime.datetime.now().date())
                return data
            else:
                log.error(f"Failed to fetch data for {symbol}: {response.status_code}")
                return None
        except Exception as e:
            log.error(f"Exception occurred while fetching data for {symbol}: {e}")
            return None

    # Task 3: Make GET request for each symbol and collect responses
    def get_stock_data_for_symbols(ti):
        symbols = ti.xcom_pull(key='symbols', task_ids='parse_symbols')
        log.info(f"Symbols pulled from XCom: {symbols}")
        if not symbols:
            raise ValueError("No symbols found to process")
        
        server_url = 'http://192.168.4.73:8000/gex/short-term'
        headers = {"x-api-key": "brjS-Io"}
        all_responses = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(fetch_symbol_data, symbol, server_url,headers): symbol for symbol in symbols}

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        all_responses.append(result)
                except Exception as e:
                    log.error(f"Error fetching data for {symbol}: {e}")
        
        log.info(f"All responses: {all_responses}")
        ti.xcom_push(key='all_responses', value=all_responses)
        
    get_stock_data = PythonOperator(
        task_id='get_stock_data',
        python_callable=get_stock_data_for_symbols
    )

    # Task 4: Post the collected data to MongoDB
    def format_data_for_post(ti):
        all_responses = ti.xcom_pull(key='all_responses', task_ids='get_stock_data')
        log.info(f"All responses pulled from XCom: {all_responses}")
        if not all_responses:
            raise ValueError("No responses to insert into MongoDB")
        
        formatted_data = json.dumps({
            "collection": "brjs-io-db-gamma-test",
            "database": "brjs-io-db-gamma",
            "dataSource": "brjs-io-db-gamma",
            "documents": all_responses
        })
        return formatted_data

    prepare_post_data = PythonOperator(
        task_id='prepare_post_data',
        python_callable=format_data_for_post
    )

    post_to_mongo_responses = SimpleHttpOperator(
        task_id='post_to_mongo_responses',
        http_conn_id='mongo_default',
        endpoint='/app/data-kauzd/endpoint/data/v1/action/insertMany',
        method='POST',
        headers={
            "Content-Type": "application/json",
            "api-key": "GgPGg4qpFg5tyqIB5u593WUOixvX62Hk09M0XKnhUpFrnXIcx0JGtbnuuWOWchd7"
        },
        data="{{ task_instance.xcom_pull(task_ids='prepare_post_data') }}",
        log_response=True
    )

    post_to_mongodb >> parse_symbols >> get_stock_data >> prepare_post_data >> post_to_mongo_responses

