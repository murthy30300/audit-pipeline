CREATE TABLE IF NOT EXISTS calls_analyzed

ENGINE = MergeTree

ORDER BY (loan_id, call_start_time)

SETTINGS allow_nullable_key = 1

AS

SELECT
    call_id,
    loan_id,
    agent_id,
    call_start_time,
    call_duration_sec,
    call_status,
    transcript,
    call_success_flag,
    call_hour,
    is_within_hours,
    contact_attempt_seq,
    _etl_loaded_at,
    now() AS _silver_updated_at

FROM
(

SELECT
    call_id,
    loan_id,
    agent_id,

    parseDateTimeBestEffortOrNull(
        toString(call_start_time)
    ) AS call_start_time,

    coalesce(call_duration_sec,0)
        AS call_duration_sec,

    upper(toString(call_status))
        AS call_status,

    toString(transcript)
        AS transcript,

    (
        upper(toString(call_status))='COMPLETED'
        AND coalesce(call_duration_sec,0) > 30
    ) AS call_success_flag,

    toHour(
        parseDateTimeBestEffortOrNull(
            toString(call_start_time)
        )
    ) AS call_hour,

    (
        toHour(
            parseDateTimeBestEffortOrNull(
                toString(call_start_time)
            )
        ) BETWEEN 8 AND 19
    ) AS is_within_hours,

    row_number() OVER
    (
        PARTITION BY loan_id
        ORDER BY
        parseDateTimeBestEffortOrNull(
            toString(call_start_time)
        ) ASC
    ) AS contact_attempt_seq,

    _etl_loaded_at,

    row_number() OVER
    (
        PARTITION BY call_id
        ORDER BY
        parseDateTimeBestEffortOrNull(
            toString(_etl_loaded_at)
        ) DESC
    ) AS rn

FROM calls_raw

)

WHERE rn = 1;