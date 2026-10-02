"""
DAG 2_datamart_update: ежедневное инкрементальное обновление витрины global_metrics в Vertica.

Берёт данные из STAGING (transactions, currencies), исключает тестовые аккаунты (номер < 0),
пересчитывает партицию за день ds (execution date, т.е. «вчера» относительно запуска)
и записывает её в <префикс>__DWH.global_metrics.

Перед расчётом ждёт, пока DAG 1_data_import закончит загрузку за тот же день.

Настройки: Airflow Connection `vertica_conn`, Variable `vertica_schema_prefix`.
SQL продублирован в src/sql/3_global_metrics_update.sql.
"""
import logging
from datetime import datetime, timedelta

import vertica_python
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor  # ждёт другой DAG

log = logging.getLogger(__name__)

VERTICA_CONN_ID = "vertica_conn"
USD_CODE = 420  # код доллара, к нему приводим все суммы

# Шаблон схем подставляется на лету: {stg} / {dwh}; дата — {ds}
# Сначала создаём таблицу витрины (если её ещё нет)
DDL_SQL = """
CREATE TABLE IF NOT EXISTS {dwh}.global_metrics (
    date_update                    date          NOT NULL,
    currency_from                  int           NOT NULL,
    amount_total                   numeric(22,3),
    cnt_transactions               int,
    avg_transactions_per_account   numeric(18,4),
    cnt_accounts_make_transactions int
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) ALL NODES
PARTITION BY date_update
"""

# Удаляем данные за день, чтобы при повторном запуске не было дублей
DELETE_SQL = """
DELETE FROM {dwh}.global_metrics
WHERE date_update = '{ds}'::date
"""

# Считаем витрину за день. Два запроса через UNION ALL:
# 1) транзакции в долларах - курс не нужен
# 2) все остальные валюты - умножаем на курс из таблицы currencies
INSERT_SQL = """
INSERT INTO {dwh}.global_metrics
    (date_update, currency_from, amount_total, cnt_transactions,
     avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT '{ds}'::date                                              AS date_update,
       t.currency_code                                           AS currency_from,
       SUM(t.amount) / 100.0                                     AS amount_total,
       COUNT(*)                                                  AS cnt_transactions,
       COUNT(*)::numeric / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
       COUNT(DISTINCT t.account_number_from)                     AS cnt_accounts_make_transactions
FROM {stg}.transactions AS t
WHERE t.transaction_dt >= '{ds}'::date
  AND t.transaction_dt <  '{ds}'::date + 1
  AND t.status = 'done'            -- берём только успешные
  AND t.account_number_from >= 0   -- тестовые аккаунты (с минусом) выкидываем
  AND t.account_number_to >= 0
  AND t.currency_code = {usd}
GROUP BY t.currency_code
UNION ALL
SELECT '{ds}'::date                                              AS date_update,
       t.currency_code                                           AS currency_from,
       SUM(t.amount * c.currency_code_div) / 100.0               AS amount_total,
       COUNT(*)                                                  AS cnt_transactions,
       COUNT(*)::numeric / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
       COUNT(DISTINCT t.account_number_from)                     AS cnt_accounts_make_transactions
FROM {stg}.transactions AS t
JOIN {stg}.currencies AS c
  ON c.currency_code = t.currency_code
 AND c.date_update::date = t.transaction_dt::date
WHERE t.transaction_dt >= '{ds}'::date
  AND t.transaction_dt <  '{ds}'::date + 1
  AND t.status = 'done'
  AND t.account_number_from >= 0
  AND t.account_number_to >= 0
  AND t.currency_code <> {usd}
  AND c.currency_code_with = {usd}
GROUP BY t.currency_code
"""


def update_global_metrics(ds: str, **_):
    # ds - день, за который считаем витрину
    prefix = Variable.get("vertica_schema_prefix", default_var="VT260909EE3191")
    # параметры для подстановки в SQL
    params = {"stg": f"{prefix}__STAGING", "dwh": f"{prefix}__DWH", "ds": ds, "usd": USD_CODE}

    # данные для подключения берём из Airflow Connection
    conn = BaseHook.get_connection(VERTICA_CONN_ID)
    with vertica_python.connect(
        host=conn.host,
        port=conn.port or 5433,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "dwh",
        autocommit=False,
    ) as connection:
        cur = connection.cursor()
        cur.execute(DDL_SQL.format(**params))      # таблица витрины, если её ещё нет
        cur.execute(DELETE_SQL.format(**params))   # идемпотентность: пересчёт дня
        cur.execute(INSERT_SQL.format(**params))
        connection.commit()  # всё в одной транзакции, если упадёт - ничего не сохранится
    log.info("global_metrics за %s обновлена", ds)


# общие настройки
default_args = {
    "owner": "iar-dubrov",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="2_datamart_update",
    description="STAGING -> DWH.global_metrics, ежедневный инкремент за вчера",
    default_args=default_args,
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval="@daily",  # каждый день
    catchup=True,  # догоняем весь октябрь
    max_active_runs=1,
    tags=["final_project", "vertica", "datamart"],
) as dag:
    # сначала ждём, пока первый DAG загрузит данные за этот же день
    wait_for_import = ExternalTaskSensor(
        task_id="wait_for_data_import",
        external_dag_id="1_data_import",
        external_task_id=None,          # ждём весь DAG целиком
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )
    update_task = PythonOperator(
        task_id="update_global_metrics",
        python_callable=update_global_metrics,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # сначала ждём загрузку, потом считаем витрину
    wait_for_import >> update_task
