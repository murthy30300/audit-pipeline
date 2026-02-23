CREATE TABLE IF NOT EXISTS messages_clean
ENGINE = MergeTree
ORDER BY (customer_id, sent_time, message_id)
SETTINGS allow_nullable_key = 1
AS

SELECT
    message_id,
    customer_id,

    upper(toString(channel)) AS channel,

    parseDateTimeBestEffortOrNull(
        toString(sent_time)
    ) AS sent_time,

    upper(
        coalesce(
            toString(delivery_status),
            toString(status),
            'UNKNOWN'
        )
    ) AS delivery_status,

    message_body,

    toHour(
        parseDateTimeBestEffortOrNull(
            toString(sent_time)
        )
    ) AS sent_hour,

    (
        toHour(
            parseDateTimeBestEffortOrNull(
                toString(sent_time)
            )
        ) BETWEEN 8 AND 19
    ) AS within_business_hours,

    _etl_loaded_at,

    now() AS _silver_updated_at

FROM
(
    SELECT *,
        row_number() OVER(
            PARTITION BY message_id
            ORDER BY parseDateTimeBestEffortOrNull(
                toString(_etl_loaded_at)
            ) DESC
        ) rn
    FROM messages_raw
)
WHERE rn = 1;