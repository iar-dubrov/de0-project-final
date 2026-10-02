import io
import logging
import re
from datetime import datetime, timedelta

import boto3  # чтобы ходить в S3
import pandas as pd  # читаем csv и фильтруем по дате
import vertica_python  # коннект к вертике
from airflow import DAG
from airflow.hooks.base import BaseHook  # достаём Connection из Airflow
from airflow.models import Variable  # достаём Variables из Airflow
from airflow.operators.python import PythonOperator

# логгер, чтобы в логах таска было видно что происходит
log = logging.getLogger(__name__)

# настройки S3, откуда берём данные (ключи тут НЕ храним, они в Variables)
S3_ENDPOINT = "https://storage.yandexcloud.net"
S3_BUCKET = "final-project"
CURRENCIES_KEY = "currencies_history.csv"
TRANSACTIONS_PREFIX = "transactions_batch_"

# имя Connection в Airflow, там хост, логин и пароль от вертики
VERTICA_CONN_ID = "vertica_conn"

# какие колонки ждём в файлах (по порядку как в таблицах)
TRANSACTIONS_COLUMNS = [
    "operation_id", "account_number_from", "account_number_to", "currency_code",
    "country", "status", "transaction_type", "amount", "transaction_dt",
]
CURRENCIES_COLUMNS = ["date_update", "currency_code", "currency_code_with", "currency_code_div"]

# DDL staging-слоя (то же самое лежит в src/sql/1_ddl_staging.sql)
# CREATE ... IF NOT EXISTS, чтобы можно было запускать много раз и ничего не ломалось
DDL_STAGING = [
    """
    CREATE TABLE IF NOT EXISTS {stg}.transactions (
        operation_id        varchar(60)  NOT NULL,
        account_number_from int          NOT NULL,
        account_number_to   int          NOT NULL,
        currency_code       int          NOT NULL,
        country             varchar(30),
        status              varchar(30),
        transaction_type    varchar(30),
        amount              int,
        transaction_dt      timestamp(3) NOT NULL
    )
    ORDER BY transaction_dt, operation_id
    -- сегментируем по хэшу от даты и id транзакции, как просили в задании
    SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES
    -- партиции по дням
    PARTITION BY transaction_dt::date
    """,
    # вторая проекция, чтобы быстрее искать по датам
    """
    CREATE PROJECTION IF NOT EXISTS {stg}.transactions_by_dt AS
    SELECT operation_id, account_number_from, account_number_to, currency_code,
           country, status, transaction_type, amount, transaction_dt
    FROM {stg}.transactions
    ORDER BY transaction_dt, currency_code, status
    SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES
    """,
    """
    CREATE TABLE IF NOT EXISTS {stg}.currencies (
        date_update        timestamp(3) NOT NULL,
        currency_code      int          NOT NULL,
        currency_code_with int          NOT NULL,
        currency_code_div  numeric(5,3) NOT NULL
    )
    ORDER BY date_update, currency_code
    SEGMENTED BY hash(date_update, currency_code) ALL NODES
    PARTITION BY date_update::date
    """,
    """
    CREATE PROJECTION IF NOT EXISTS {stg}.currencies_by_dt AS
    SELECT date_update, currency_code, currency_code_with, currency_code_div
    FROM {stg}.currencies
    ORDER BY date_update, currency_code_with
    SEGMENTED BY hash(date_update, currency_code) ALL NODES
    """,
]


def _schema_prefix() -> str:
    # префикс схемы лежит в Variable, чтобы не хардкодить в нескольких местах
    return Variable.get("vertica_schema_prefix", default_var="VT260909EE3191")


def _s3_variable(*names: str) -> str:
    """Первая найденная Airflow Variable из списка имён."""
    # у разных людей ключи называются по-разному, поэтому перебираем варианты
    for name in names:
        value = Variable.get(name, default_var=None)
        if value and value.strip():
            return value.strip()   
    raise KeyError(f"Не найдена ни одна из Airflow Variables: {names}")


def _s3_client():
    # создаём клиент S3, ключи берём из Variables
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=_s3_variable("s3_access_key_id", "aws_access_key_id"),
        aws_secret_access_key=_s3_variable("s3_secret_access_key", "aws_secret_access_key"),
    )


def _vertica_connection():
    # берём данные подключения из Airflow Connection
    conn = BaseHook.get_connection(VERTICA_CONN_ID)
    info = {
        "host": conn.host,
        "port": conn.port or 5433,  # 5433 - стандартный порт вертики
        "user": conn.login,
        "password": conn.password,
        "database": conn.schema or "dwh",
        "autocommit": False,  
    }
    return vertica_python.connect(**info)


# В реальном файле колонка курса называется currency_with_div — приводим к имени из модели данных
COLUMN_ALIASES = {"currency_with_div": "currency_code_div"}


def _read_csv(s3, key: str) -> pd.DataFrame:
    # скачиваем файл целиком в память и читаем pandas-ом
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return pd.read_csv(io.BytesIO(body)).rename(columns=COLUMN_ALIASES)


def _check_columns(df: pd.DataFrame, expected: list, key: str) -> None:
    # смотрим, что всё нужное есть, иначе сразу понятная ошибка
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"В файле {key} нет колонок {missing}; есть: {list(df.columns)}")


def _copy_to_vertica(df: pd.DataFrame, table: str, columns: list, delete_sql: str, ds: str) -> int:
    """Идемпотентно заменяет в таблице данные за дату ds."""
    # складываем датафрейм в csv в памяти, чтобы отдать в COPY
    buffer = io.StringIO()
    df[columns].to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    copy_sql = (
        f"COPY {table} ({', '.join(columns)}) FROM STDIN "
        "DELIMITER ',' ENCLOSED BY '\"' NULL AS ''"
    )
    with _vertica_connection() as connection:
        cur = connection.cursor()
        # сначала удаляем данные за этот день, чтобы при перезапуске не было дублей
        cur.execute(delete_sql)
        if len(df):
            cur.copy(copy_sql, buffer.getvalue())
        connection.commit()
    log.info("%s: за %s загружено строк: %s", table, ds, len(df))
    return len(df)


def create_staging_tables(**_):
    """Создаёт таблицы staging-слоя, если их ещё нет."""
    stg = f"{_schema_prefix()}__STAGING"
    with _vertica_connection() as connection:
        cur = connection.cursor()
        for statement in DDL_STAGING:
            cur.execute(statement.format(stg=stg))
        connection.commit()
    log.info("Таблицы в %s готовы", stg)


def load_currencies(ds: str, **_):
    """Курсы валют за дату ds."""
    # ds - дата запуска в формате YYYY-MM-DD
    s3 = _s3_client()
    df = _read_csv(s3, CURRENCIES_KEY)
    _check_columns(df, CURRENCIES_COLUMNS, CURRENCIES_KEY)
    df["date_update"] = pd.to_datetime(df["date_update"])
    # оставляем только строки за нужный день
    df = df[df["date_update"].dt.strftime("%Y-%m-%d") == ds]

    table = f"{_schema_prefix()}__STAGING.currencies"
    delete_sql = f"DELETE FROM {table} WHERE date_update::date = '{ds}'::date"
    _copy_to_vertica(df, table, CURRENCIES_COLUMNS, delete_sql, ds)


def _batch_number(key: str) -> int:
    # достаём номер из названия файла, чтобы батчи шли по порядку (1,2,...,10, а не 1,10,2)
    match = re.search(r"(\d+)", key[len(TRANSACTIONS_PREFIX):])
    return int(match.group(1)) if match else 0


def load_transactions(ds: str, **_):
    """Транзакции за дату ds: перебираем все батчи и забираем строки нужного дня."""
    s3 = _s3_client()
    # смотрим какие вообще есть файлы с транзакциями
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=TRANSACTIONS_PREFIX)
    keys = sorted(
        (obj["Key"] for page in pages for obj in page.get("Contents", [])),
        key=_batch_number,
    )
    if not keys:
        raise ValueError(f"В бакете {S3_BUCKET} нет файлов {TRANSACTIONS_PREFIX}*.csv")

    parts = []  # сюда складываем куски за нужный день из каждого батча
    for key in keys:
        df = _read_csv(s3, key)
        _check_columns(df, TRANSACTIONS_COLUMNS, key)
        df["transaction_dt"] = pd.to_datetime(df["transaction_dt"])
        day = df["transaction_dt"].dt.strftime("%Y-%m-%d")
        # Внутри батчей встречаются строки за разные дни (в т.ч. «поздние» события),
        # поэтому читаем все батчи и берём из каждого строки за нужную дату.
        parts.append(df[day == ds])

    # склеиваем всё в одну таблицу и грузим
    result = pd.concat(parts, ignore_index=True)
    table = f"{_schema_prefix()}__STAGING.transactions"
    delete_sql = f"DELETE FROM {table} WHERE transaction_dt::date = '{ds}'::date"
    _copy_to_vertica(result, table, TRANSACTIONS_COLUMNS, delete_sql, ds)


# общие настройки тасков
default_args = {
    "owner": "iar-dubrov",
    "retries": 2,  # если упало - пробуем ещё раз
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="1_data_import",
    description="S3 -> Vertica STAGING (transactions, currencies), загрузка по дням",
    default_args=default_args,
    start_date=datetime(2022, 10, 1),  # данные начинаются с октября 2022
    end_date=datetime(2022, 10, 31),
    schedule_interval="@daily",  # раз в день
    catchup=True,  # чтобы догнать все дни октября
    max_active_runs=1,  # по одному дню за раз, чтобы не нагружать вертику
    tags=["final_project", "s3", "vertica", "staging"],
) as dag:
    create_tables_task = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables,
    )
    load_currencies_task = PythonOperator(
        task_id="load_currencies",
        python_callable=load_currencies,
        op_kwargs={"ds": "{{ ds }}"},
    )
    load_transactions_task = PythonOperator(
        task_id="load_transactions",
        python_callable=load_transactions,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # порядок: сначала таблицы, потом курсы, потом транзакции
    create_tables_task >> load_currencies_task >> load_transactions_task
