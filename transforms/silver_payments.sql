-- Silver transform for payments
-- Source: payments_raw (Bronze)
-- Target: payments_clean (Silver)

CREATE TABLE IF NOT EXISTS payments_clean
ENGINE = MergeTree
ORDER BY (loan_id, payment_date, payment_id) AS
SELECT
    payment_id,
    loan_id,
    customer_id,
    amount,
    emi_amount,
    payment_date,
    payment_status,
    bounce_flag,
    partial_pay_flag,
    collection_efficiency,
    _etl_loaded_at,
    now() AS _silver_updated_at
FROM
(
    SELECT
        p.payment_id,
        p.loan_id,
        p.customer_id,
        toFloat64OrZero(p.amount) AS amount,
        toFloat64OrZero(l.emi_amount) AS emi_amount,
        toDateOrNull(p.payment_date) AS payment_date,
        upper(coalesce(toString(p.gateway_status), toString(p.status))) AS gateway_status_normalized,
        multiIf(
            upper(coalesce(toString(p.gateway_status), toString(p.status))) IN ('SUCCESS', 'SUCCEEDED', 'OK', 'PAID'), 'SUCCESS',
            upper(coalesce(toString(p.gateway_status), toString(p.status))) IN ('FAILED', 'FAIL', 'ERROR'), 'FAILED',
            upper(coalesce(toString(p.gateway_status), toString(p.status))) IN ('PENDING', 'IN_PROCESS', 'PROCESSING'), 'PENDING',
            upper(coalesce(toString(p.gateway_status), toString(p.status))) IN ('BOUNCE', 'RETURN', 'NSF', 'BOUNCED'), 'BOUNCED',
            'PENDING'
        ) AS payment_status,
        (
            upper(coalesce(toString(p.gateway_status), toString(p.status))) IN ('BOUNCE', 'RETURN', 'NSF', 'BOUNCED')
        ) AS bounce_flag,
        (
            toFloat64OrZero(l.emi_amount) > 0
            AND toFloat64OrZero(p.amount) < toFloat64OrZero(l.emi_amount)
        ) AS partial_pay_flag,
        if(
            toFloat64OrZero(l.emi_amount) > 0,
            (toFloat64OrZero(p.amount) / toFloat64OrZero(l.emi_amount)) * 100,
            0.0
        ) AS collection_efficiency,
        p._etl_loaded_at,
        row_number() OVER (
            PARTITION BY p.payment_id
            ORDER BY parseDateTimeBestEffortOrNull(toString(p._etl_loaded_at)) DESC
        ) AS rn
    FROM payments_raw AS p
    LEFT JOIN loans_clean AS l
        ON p.loan_id = l.loan_id
)
WHERE rn = 1;
