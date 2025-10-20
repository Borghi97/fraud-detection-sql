--------------------------------------------------------------------------------
-- View 1: Base classified transactions
-- Converts transaction_date to TIMESTAMP, classifies each transaction into 
-- LOW / MED / HIGH based on percentile thresholds, and flags chargebacks.
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_classified_transactions AS
WITH base AS (
    SELECT
        transaction_id,
        merchant_id,
        user_id,
        card_number,
        TO_TIMESTAMP(transaction_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF') AS transaction_ts,
        transaction_amount,
        device_id,
        CASE WHEN has_cbk = 'TRUE' THEN 1 ELSE 0 END AS has_cbk
    FROM t_sample
),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY transaction_amount) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY transaction_amount) AS q3
    FROM base
)
SELECT
    b.*,
    CASE
        WHEN b.transaction_amount < p.q1 THEN 'LOW'
        WHEN b.transaction_amount BETWEEN p.q1 AND p.q3 THEN 'MED'
        ELSE 'HIGH'
    END AS transaction_class
FROM base b
CROSS JOIN percentiles p;


--------------------------------------------------------------------------------
-- View 2: Rapid transaction analysis
-- Identifies rapid consecutive transactions by the same user or same device.
-- Flags them if they occur within 5 minutes.
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_rapid_transactions AS
WITH next_user_ts AS (
    SELECT
        c.*,
        LEAD(transaction_ts) OVER (PARTITION BY user_id ORDER BY transaction_ts) AS next_ts_user
    FROM v_classified_transactions c
),
next_device_ts AS (
    SELECT
        n.*,
        LEAD(transaction_ts) OVER (PARTITION BY device_id ORDER BY transaction_ts) AS next_ts_device
    FROM next_user_ts n
)
SELECT
    n.*,
    CASE
        WHEN next_ts_user IS NOT NULL 
             AND (CAST(next_ts_user AS DATE) - CAST(transaction_ts AS DATE)) * 24 * 60 <= 5
        THEN 1 ELSE 0
    END AS rapid_user_transaction,
    CASE
        WHEN next_ts_device IS NOT NULL 
             AND (CAST(next_ts_device AS DATE) - CAST(transaction_ts AS DATE)) * 24 * 60 <= 5
        THEN 1 ELSE 0
    END AS rapid_device_transaction
FROM next_device_ts n;


--------------------------------------------------------------------------------
-- Query 1: Count by transaction class
-- Shows how many transactions exist per class and how many were chargebacks.
--------------------------------------------------------------------------------
SELECT
    transaction_class,
    COUNT(*) AS total_transactions,
    SUM(has_cbk) AS total_chargebacks
FROM v_rapid_transactions
GROUP BY transaction_class;


--------------------------------------------------------------------------------
-- Query 2: Count of rapid transactions
-- Counts how many transactions happened within 5 minutes for user/device.
--------------------------------------------------------------------------------
SELECT
    SUM(rapid_user_transaction) AS rapid_user_total,
    SUM(rapid_device_transaction) AS rapid_device_total
FROM v_rapid_transactions;


--------------------------------------------------------------------------------
-- Query 3: Transactions with potential fraud
-- Detects transactions that are HIGH value, rapid, and have a chargeback.
--------------------------------------------------------------------------------
SELECT *
FROM v_rapid_transactions
WHERE transaction_class = 'HIGH'
  AND (rapid_user_transaction = 1 OR rapid_device_transaction = 1)
  AND has_cbk = 1
ORDER BY transaction_ts;


CREATE OR REPLACE VIEW v_fraud_summary AS               
SELECT
    transaction_class,                                 
    COUNT(*) AS total_transactions,                     
    SUM(has_cbk) AS total_chargebacks,                  
    SUM(rapid_user_transaction) AS total_rapid_user,   
    SUM(rapid_device_transaction) AS total_rapid_device,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,    
    ROUND(100 * SUM(has_cbk) / COUNT(*), 2) AS cbk_rate_percent, 
    ROUND(100 * (SUM(rapid_user_transaction) + SUM(rapid_device_transaction)) 
          / (2 * COUNT(*)), 2) AS rapid_rate_percent    
FROM v_rapid_transactions                              
GROUP BY transaction_class; 

-- Query: Fraud summary overview
-- Shows a summary of fraud metrics by transaction class

SELECT 
    transaction_class,
    total_transactions,
    total_chargebacks,
    total_rapid_user,
    total_rapid_device,
    avg_amount,
    cbk_rate_percent,
    rapid_rate_percent
FROM 
    v_fraud_summary
ORDER BY 
    cbk_rate_percent DESC;
    
    SELECT *
FROM v_rapid_transactions
WHERE transaction_class = 'HIGH'
  AND (rapid_user_transaction = 1 OR rapid_device_transaction = 1)
  AND has_cbk = 1
ORDER BY transaction_ts;
