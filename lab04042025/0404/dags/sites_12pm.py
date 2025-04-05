from urllib import request
from datetime import datetime, timedelta
import logging

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException

# Configure logging
logger = logging.getLogger(__name__)

# Define constants
RUSSIAN_SITES = {"Яндекс", "ВКонтакте", "Mail.ru"}
DATA_FILE_PATH = "/tmp/wikipageviews"
SQL_FILE_PATH = "/tmp/postgres_query.sql"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=90),
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "catchup": True,
}

dag = DAG(
    dag_id="sites_12pm",
    default_args=default_args,
    schedule_interval="0 12 * * *",  
    template_searchpath="/tmp",
    max_active_runs=1,
)


def _get_data(year, month, day, output_path):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-120000.gz"
    )
    try:
        logger.info(f"Downloading data from: {url}")
        request.urlretrieve(url, output_path)
        return True
    except Exception as e:
        logger.error(f"Failed to download data: {e}")
        return False


def _get_data_wrapper(**context):
    """Wrapper function to handle execution date and errors."""
    execution_date = context["execution_date"]
    success = _get_data(
        year=execution_date.year,
        month=execution_date.month,
        day=execution_date.day,
        output_path="/tmp/wikipageviews.gz"
    )
    if not success:
        raise AirflowSkipException("Data not available for this time period")


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data_wrapper,
    provide_context=True,
    dag=dag,
)


extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command=f"gunzip --force /tmp/wikipageviews.gz || echo 'No file to extract'",
    dag=dag,
)


def _fetch_pageviews(pagenames, execution_date, **context):
    """Process pageviews data and generate SQL queries."""
    result = dict.fromkeys(pagenames, 0)
    try:
        with open(DATA_FILE_PATH, "r") as f:
            for line in f:
                try:
                    domain_code, page_title, view_counts, _ = line.split(" ")
                    if domain_code == "ru" and page_title in pagenames:
                        result[page_title] = view_counts
                except ValueError:
                    logger.warning(f"Malformed line: {line}")
                    continue

        with open(SQL_FILE_PATH, "w") as f:
            for pagename, pageviewcount in result.items():
                f.write(
                    "INSERT INTO pageview_counts VALUES ("
                    f"'{pagename}', {pageviewcount}, '{execution_date}'"
                    ");\n"
                )
        logger.info(f"Processed data for {execution_date}")
    except FileNotFoundError:
        logger.warning("No pageviews file found, skipping insert")
        with open(SQL_FILE_PATH, "w") as f:
            f.write("-- No data for this period\n")


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": RUSSIAN_SITES},
    provide_context=True,
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews >> write_to_postgres