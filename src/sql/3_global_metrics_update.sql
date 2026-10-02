-- Шаг 1. Делаем загрузку идемпотентной: удаляем партицию за этот день
DELETE FROM VT260909EE3191__DWH.global_metrics
WHERE date_update = '{ds}'::date;

-- Шаг 2. Считаем агрегаты за день.
-- Ветка 1: транзакции в долларах (конвертация не нужна).
-- Ветка 2: транзакции в остальных валютах (конвертация по курсу на дату транзакции).
INSERT INTO VT260909EE3191__DWH.global_metrics
    (date_update, currency_from, amount_total, cnt_transactions,
     avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT '{ds}'::date                                              AS date_update,
       t.currency_code                                           AS currency_from,
       SUM(t.amount) / 100.0                                     AS amount_total,
       COUNT(*)                                                  AS cnt_transactions,
       COUNT(*)::numeric / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
       COUNT(DISTINCT t.account_number_from)                     AS cnt_accounts_make_transactions
FROM VT260909EE3191__STAGING.transactions AS t
WHERE t.transaction_dt >= '{ds}'::date
  AND t.transaction_dt <  '{ds}'::date + 1
  AND t.status = 'done'
  AND t.account_number_from >= 0
  AND t.account_number_to >= 0
  AND t.currency_code = 420
GROUP BY t.currency_code
UNION ALL
SELECT '{ds}'::date                                              AS date_update,
       t.currency_code                                           AS currency_from,
       SUM(t.amount * c.currency_code_div) / 100.0               AS amount_total,
       COUNT(*)                                                  AS cnt_transactions,
       COUNT(*)::numeric / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
       COUNT(DISTINCT t.account_number_from)                     AS cnt_accounts_make_transactions
FROM VT260909EE3191__STAGING.transactions AS t
JOIN VT260909EE3191__STAGING.currencies AS c
  ON c.currency_code = t.currency_code
 AND c.date_update::date = t.transaction_dt::date
WHERE t.transaction_dt >= '{ds}'::date
  AND t.transaction_dt <  '{ds}'::date + 1
  AND t.status = 'done'
  AND t.account_number_from >= 0
  AND t.account_number_to >= 0
  AND t.currency_code <> 420
  AND c.currency_code_with = 420
GROUP BY t.currency_code;
