-- Silver transform for loans
-- Source: loans_raw (Bronze)
-- Target: loans_clean (Silver)
CREATE TABLE IF NOT EXISTS loans_clean ENGINE = MergeTree
ORDER BY
    loan_id AS
SELECT
    loan_id,
    borrower_id,
    principal_amount,
    emi_amount,
    COALESCE(tenure_months, 12) AS tenure_months,
    loan_status,
    due_date,
    disbursement_date,
    total_paid,
    dpd_days,
    loan_aging_bucket,
    npa_flag,
    overdue_amount,
    _etl_loaded_at,
    now () AS _silver_updated_at
FROM
    (
        SELECT
            loan_id,
            borrower_id,
            principal_amount,
            emi_amount,
            tenure_months,
            upper(toString (loan_status)) AS loan_status,
            -- FIX: toString() before toDateOrNull() because columns are String in Bronze
            coalesce(
                toDateOrNull (toString (due_date)),
                toDateOrNull (toString (next_due_date))
            ) AS due_date,
            toDateOrNull (toString (disbursement_date)) AS disbursement_date,
            total_paid,
            _etl_loaded_at,
            -- dpd_days
            if (
                upper(toString (loan_status)) = 'CLOSED'
                OR coalesce(
                    toDateOrNull (toString (due_date)),
                    toDateOrNull (toString (next_due_date))
                ) IS NULL,
                0,
                greatest (
                    dateDiff (
                        'day',
                        coalesce(
                            toDateOrNull (toString (due_date)),
                            toDateOrNull (toString (next_due_date))
                        ),
                        today ()
                    ),
                    0
                )
            ) AS dpd_days,
            -- loan_aging_bucket
            multiIf (
                if (
                    upper(toString (loan_status)) = 'CLOSED'
                    OR coalesce(
                        toDateOrNull (toString (due_date)),
                        toDateOrNull (toString (next_due_date))
                    ) IS NULL,
                    0,
                    greatest (
                        dateDiff (
                            'day',
                            coalesce(
                                toDateOrNull (toString (due_date)),
                                toDateOrNull (toString (next_due_date))
                            ),
                            today ()
                        ),
                        0
                    )
                ) <= 0,
                'CURRENT',
                if (
                    upper(toString (loan_status)) = 'CLOSED'
                    OR coalesce(
                        toDateOrNull (toString (due_date)),
                        toDateOrNull (toString (next_due_date))
                    ) IS NULL,
                    0,
                    greatest (
                        dateDiff (
                            'day',
                            coalesce(
                                toDateOrNull (toString (due_date)),
                                toDateOrNull (toString (next_due_date))
                            ),
                            today ()
                        ),
                        0
                    )
                ) <= 30,
                '1-30 DPD',
                if (
                    upper(toString (loan_status)) = 'CLOSED'
                    OR coalesce(
                        toDateOrNull (toString (due_date)),
                        toDateOrNull (toString (next_due_date))
                    ) IS NULL,
                    0,
                    greatest (
                        dateDiff (
                            'day',
                            coalesce(
                                toDateOrNull (toString (due_date)),
                                toDateOrNull (toString (next_due_date))
                            ),
                            today ()
                        ),
                        0
                    )
                ) <= 60,
                '31-60 DPD',
                if (
                    upper(toString (loan_status)) = 'CLOSED'
                    OR coalesce(
                        toDateOrNull (toString (due_date)),
                        toDateOrNull (toString (next_due_date))
                    ) IS NULL,
                    0,
                    greatest (
                        dateDiff (
                            'day',
                            coalesce(
                                toDateOrNull (toString (due_date)),
                                toDateOrNull (toString (next_due_date))
                            ),
                            today ()
                        ),
                        0
                    )
                ) <= 90,
                '61-90 DPD',
                'NPA'
            ) AS loan_aging_bucket,
            -- npa_flag
            (
                if (
                    upper(toString (loan_status)) = 'CLOSED'
                    OR coalesce(
                        toDateOrNull (toString (due_date)),
                        toDateOrNull (toString (next_due_date))
                    ) IS NULL,
                    0,
                    greatest (
                        dateDiff (
                            'day',
                            coalesce(
                                toDateOrNull (toString (due_date)),
                                toDateOrNull (toString (next_due_date))
                            ),
                            today ()
                        ),
                        0
                    )
                ) > 90
                OR upper(toString (loan_status)) = 'NPA'
            ) AS npa_flag,
            principal_amount - coalesce(total_paid, 0) AS overdue_amount,
            row_number() OVER (
                PARTITION BY
                    loan_id
                ORDER BY
                    parseDateTimeBestEffortOrNull (toString (_etl_loaded_at)) DESC
            ) AS rn
        FROM
            loans_raw
    )
WHERE
    rn = 1;