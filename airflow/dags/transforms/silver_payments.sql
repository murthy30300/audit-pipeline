CREATE TABLE IF NOT EXISTS payments_clean
ENGINE = MergeTree
ORDER BY (loan_id, payment_date, payment_id)
SETTINGS allow_nullable_key = 1
AS

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

        -- already float
        p.amount AS amount,

        -- EMI from loans table
        l.emi_amount AS emi_amount,

        -- safe date cast
        toDateOrNull(toString(p.payment_date)) AS payment_date,

        ------------------------------------------------
        -- NORMALIZED STATUS (FIXED)
        ------------------------------------------------
        upper(toString(p.payment_status))
            AS payment_status_normalized,

        ------------------------------------------------
        -- FINAL PAYMENT STATUS
        ------------------------------------------------
        multiIf(

            payment_status_normalized IN
            ('SUCCESS','SUCCEEDED','OK','PAID'),
            'SUCCESS',

            payment_status_normalized IN
            ('FAILED','FAIL','ERROR'),
            'FAILED',

            payment_status_normalized IN
            ('PENDING','IN_PROCESS','PROCESSING'),
            'PENDING',

            payment_status_normalized IN
            ('BOUNCE','RETURN','NSF','BOUNCED'),
            'BOUNCED',

            'PENDING'
        ) AS payment_status,

        ------------------------------------------------
        -- BOUNCE FLAG
        ------------------------------------------------
        payment_status_normalized IN
        ('BOUNCE','RETURN','NSF','BOUNCED')
        AS bounce_flag,

        ------------------------------------------------
        -- PARTIAL PAYMENT
        ------------------------------------------------
        (
            coalesce(l.emi_amount,0) > 0
            AND coalesce(p.amount,0)
            < coalesce(l.emi_amount,0)
        ) AS partial_pay_flag,

        ------------------------------------------------
        -- COLLECTION EFFICIENCY
        ------------------------------------------------
        if(
            coalesce(l.emi_amount,0) > 0,
            (coalesce(p.amount,0)
             / coalesce(l.emi_amount,1)) * 100,
            0.0
        ) AS collection_efficiency,

        p._etl_loaded_at,

        ------------------------------------------------
        -- DEDUP LATEST RECORD
        ------------------------------------------------
        row_number() OVER
        (
            PARTITION BY p.payment_id
            ORDER BY
            parseDateTimeBestEffortOrNull(
                toString(p._etl_loaded_at)
            ) DESC
        ) AS rn

    FROM payments_raw AS p

    LEFT JOIN loans_clean AS l
        ON p.loan_id = l.loan_id

)

WHERE rn = 1;