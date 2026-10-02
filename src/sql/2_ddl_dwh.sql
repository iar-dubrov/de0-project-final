CREATE TABLE IF NOT EXISTS VT260909EE3191__DWH.global_metrics (
    date_update                    date          NOT NULL, -- дата расчёта
    currency_from                  int           NOT NULL, -- код валюты транзакции
    amount_total                   numeric(22,3),          -- общая сумма транзакций по валюте в долларах
    cnt_transactions               int,                    -- общий объём транзакций по валюте
    avg_transactions_per_account   numeric(18,4),          -- средний объём транзакций с аккаунта
    cnt_accounts_make_transactions int                     -- уникальные аккаунты с транзакциями по валюте
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) ALL NODES
PARTITION BY date_update;
