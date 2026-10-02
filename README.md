# Итоговый проект «Инженер данных»: транзакции и курсы валют в Vertica

Пайплайн для финтех-стартапа с международными переводами: данные о транзакциях и курсах валют забираются из S3, складываются в staging-слой хранилища Vertica, а затем по ним ежедневно, инкрементом, пересчитывается витрина `global_metrics` для аналитиков.

Оркестрация — Apache Airflow (2 DAG), хранилище — Vertica, источник — открытый S3-бакет `final-project` (вариант 1 из задания).

## Результат

Итоговая витрина лежит в Vertica:

| Что | Значение |
|---|---|
| База | `dwh` (`vertica.data-engineer.education-services.ru:5433`) |
| Схема | `VT260909EE3191__DWH` |
| Таблица витрины | `VT260909EE3191__DWH.global_metrics` |
| Staging-таблицы | `VT260909EE3191__STAGING.transactions`, `VT260909EE3191__STAGING.currencies` |
| Период | октябрь 2022 (31 запуск каждого DAG, все `success`) |

```
vertica://vt260909ee3191@vertica.data-engineer.education-services.ru:5433/dwh
таблица: VT260909EE3191__DWH.global_metrics
```

## Архитектура

```
S3 (final-project)                    Vertica
 ├─ transactions_batch_1..10.csv ─┐
 └─ currencies_history.csv        │   DAG 1_data_import          DAG 2_datamart_update
                                  └──► STAGING.transactions ──┐
                                       STAGING.currencies  ───┴──► DWH.global_metrics
```

1. `1_data_import` — на каждый день (`ds`) читает файлы из S3 через `boto3`, оставляет строки за этот день и загружает их в staging через `COPY`.
2. `2_datamart_update` — ждёт, пока `1_data_import` закончит тот же день (`ExternalTaskSensor`), и пересчитывает партицию этого дня в витрине.

Оба DAG: `schedule_interval="@daily"`, `start_date=2022-10-01`, `end_date=2022-10-31`, `catchup=True`, `max_active_runs=1`.

## Структура репозитория

```
src/
├── dags/
│   ├── 1_data_import.py        # DAG загрузки S3 -> STAGING
│   └── 2_datamart_update.py    # DAG обновления витрины global_metrics
└── sql/
    ├── 1_ddl_staging.sql       # DDL staging-слоя (таблицы + проекции)
    ├── 2_ddl_dwh.sql           # DDL витрины
    └── 3_global_metrics_update.sql  # SQL инкрементального пересчёта за день
README.md
```

## Staging-слой

Схема `VT260909EE3191__STAGING` создана в предыдущих спринтах, новые схемы не создаются — DAG только выполняет `CREATE TABLE IF NOT EXISTS`.

**`transactions`** — поля: `operation_id varchar(60)`, `account_number_from int`, `account_number_to int`, `currency_code int`, `country varchar(30)`, `status varchar(30)`, `transaction_type varchar(30)`, `amount int`, `transaction_dt timestamp(3)`.

- сортировка и сегментирование: `ORDER BY transaction_dt, operation_id`, `SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES`;
- партиции по дням: `PARTITION BY transaction_dt::date`;
- дополнительная проекция по датам `transactions_by_dt` (`ORDER BY transaction_dt, currency_code, status`).

**`currencies`** — поля: `date_update timestamp(3)`, `currency_code int`, `currency_code_with int`, `currency_code_div numeric(5,3)`.

- `ORDER BY date_update, currency_code`, `SEGMENTED BY hash(date_update, currency_code) ALL NODES`, `PARTITION BY date_update::date`;
- дополнительная проекция по датам `currencies_by_dt`.

В файле `currencies_history.csv` колонка курса называется `currency_with_div`; при чтении она переименовывается в `currency_code_div`, как в модели данных из задания.

## DAG `1_data_import`

Задачи: `create_staging_tables >> load_currencies >> load_transactions`.

- Батчи `transactions_batch_*.csv` обрабатываются итеративно, по порядковому номеру. В батчах встречаются строки за разные дни, поэтому каждый запуск читает все батчи и берёт из них строки за свою дату.
- Загрузка идемпотентна: перед `COPY` удаляются данные за этот день, поэтому перезапуск не создаёт дубли.

## DAG `2_datamart_update`

Задачи: `wait_for_data_import (ExternalTaskSensor) >> update_global_metrics`.

За день `ds` (execution date — то есть «вчера» относительно момента фактического запуска) в одной транзакции выполняются `CREATE TABLE IF NOT EXISTS`, `DELETE` строк этого дня и `INSERT` новых. Так витрина пополняется инкрементально — по одной партиции за день.

### Витрина `global_metrics`

| Поле | Смысл |
|---|---|
| `date_update` | дата расчёта |
| `currency_from` | код валюты транзакции |
| `amount_total` | общая сумма транзакций по валюте в долларах |
| `cnt_transactions` | общее количество транзакций по валюте |
| `avg_transactions_per_account` | среднее количество транзакций на аккаунт-отправитель |
| `cnt_accounts_make_transactions` | количество уникальных аккаунтов, совершивших транзакции в этой валюте |

Партиционирована по `date_update`, сегментирована по `hash(date_update, currency_from)`.

### Допущения (где условие задачи оставляет свободу)

- В расчёт идут только транзакции со статусом `done` (успешно выполненные); остальные статусы (`queued`, `in_progress`, `blocked`, `chargeback`) не считаются оборотом.
- Тестовые аккаунты исключаются: `account_number_from >= 0` и `account_number_to >= 0`.
- `amount` хранится в минимальных единицах валюты (копейки, центы), поэтому сумма делится на 100.
- Доллар США — код `420`: его курс к самому себе равен 1, конвертация не нужна.
- Остальные валюты переводятся в доллары по курсу `currency_code_div` из строки справочника, где `currency_code` = валюта транзакции, `currency_code_with` = 420, а `date_update` — дата транзакции.
- Среднее считается как число транзакций / число уникальных аккаунтов-отправителей.
- Строка в витрине появляется только для тех дат и валют, где нашлись подходящие транзакции. Поэтому за октябрь в витрине 68 строк за 10 дней.
