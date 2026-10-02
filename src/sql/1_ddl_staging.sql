-- STAGING-слой: сырые данные из S3 (бакет final-project).
-- Схема уже создана в предыдущих спринтах, новые схемы не создаём.
-- Имя схемы: VT260909EE3191__STAGING (логин в Vertica) — при необходимости замените префикс.

-- Транзакции
CREATE TABLE IF NOT EXISTS VT260909EE3191__STAGING.transactions (
    operation_id       varchar(60)  NOT NULL,   -- id транзакции
    account_number_from int         NOT NULL,   -- счёт отправителя
    account_number_to   int         NOT NULL,   -- счёт получателя
    currency_code       int         NOT NULL,   -- код валюты страны-источника
    country             varchar(30),            -- страна-источник
    status              varchar(30),            -- queued / in_progress / blocked / done / chargeback
    transaction_type    varchar(30),            -- тип транзакции во внутреннем учёте
    amount              int,                    -- сумма в минимальной единице валюты
    transaction_dt      timestamp(3) NOT NULL   -- дата и время транзакции
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt::date;

-- Дополнительная проекция по датам (быстрые выборки за день)
CREATE PROJECTION IF NOT EXISTS VT260909EE3191__STAGING.transactions_by_dt AS
SELECT operation_id, account_number_from, account_number_to, currency_code,
       country, status, transaction_type, amount, transaction_dt
FROM VT260909EE3191__STAGING.transactions
ORDER BY transaction_dt, currency_code, status
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES;

-- История курсов валют
CREATE TABLE IF NOT EXISTS VT260909EE3191__STAGING.currencies (
    date_update        timestamp(3) NOT NULL,   -- дата обновления курса
    currency_code      int          NOT NULL,   -- код валюты транзакции
    currency_code_with int          NOT NULL,   -- код валюты, к которой считается курс
    currency_code_div  numeric(5,3) NOT NULL    -- отношение единицы валюты к единице валюты транзакции
)
ORDER BY date_update, currency_code
SEGMENTED BY hash(date_update, currency_code) ALL NODES
PARTITION BY date_update::date;

CREATE PROJECTION IF NOT EXISTS VT260909EE3191__STAGING.currencies_by_dt AS
SELECT date_update, currency_code, currency_code_with, currency_code_div
FROM VT260909EE3191__STAGING.currencies
ORDER BY date_update, currency_code_with
SEGMENTED BY hash(date_update, currency_code) ALL NODES;
