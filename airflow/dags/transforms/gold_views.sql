

CREATE TABLE IF NOT EXISTS lender_portfolio_summary
(
    lender_id String,
    loan_aging_bucket String,
    total_loans_count UInt64,
    total_principal_disbursed Float64,
    npa_count UInt64,
    npa_amount Float64,
    total_collected_today Float64,
    collection_efficiency_pct Float64
)
ENGINE = SummingMergeTree()
ORDER BY (lender_id, loan_aging_bucket);


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_lender_portfolio_summary
TO lender_portfolio_summary
AS
SELECT

    'unknown' AS lender_id,

    l.loan_aging_bucket,

    count() AS total_loans_count,

    sum(ifNull(l.principal_amount,0)) AS total_principal_disbursed,

    sum(if(ifNull(l.npa_flag,0)=1,1,0)) AS npa_count,

    sum(
        if(ifNull(l.npa_flag,0)=1,
            ifNull(l.principal_amount,0),
            0
        )
    ) AS npa_amount,

    sumIf(
        ifNull(p.amount,0),
        toDate(p.payment_date)=today()
    ) AS total_collected_today,

    if(
        sum(ifNull(l.principal_amount,0))=0,
        0,
        sum(ifNull(l.total_paid,0))
        /
        sum(ifNull(l.principal_amount,0))*100
    ) AS collection_efficiency_pct

FROM loans_clean l

LEFT JOIN payments_clean p
ON l.loan_id=p.loan_id

GROUP BY
    l.loan_aging_bucket;



/* ===========================================================
 2. AGENT ASSIGNED LOANS
=========================================================== */

CREATE TABLE IF NOT EXISTS agent_assigned_loans
(
    agent_id String,
    loan_id String,
    borrower_name String,
    dpd_days Int32,
    overdue_amount Float64,
    loan_aging_bucket String,
    last_call_at DateTime,
    last_call_status String,
    last_call_duration UInt32,
    followup_due UInt8,
    calls_today UInt32
)
ENGINE = ReplacingMergeTree()
ORDER BY (agent_id, loan_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agent_assigned_loans
TO agent_assigned_loans
AS
SELECT

    ifNull(a.agent_id,'unassigned') AS agent_id,

    l.loan_id AS loan_id,

    ifNull(ag.name,'') AS borrower_name,

    toInt32(ifNull(l.dpd_days,0)) AS dpd_days,

    ifNull(l.overdue_amount,0) AS overdue_amount,

    l.loan_aging_bucket AS loan_aging_bucket,

    max(c.call_start_time) AS last_call_at,

    any(c.call_status) AS last_call_status,

    toUInt32(any(ifNull(c.call_duration_sec,0)))
        AS last_call_duration,

    toUInt8(

        (max(c.call_start_time)
        < now()-INTERVAL 1 DAY)

        AND

        upper(ifNull(l.loan_status,''))='ACTIVE'

    ) AS followup_due,

    countIf(
        c.call_id,
        toDate(c.call_start_time)=today()
    ) AS calls_today

FROM loans_clean l

LEFT JOIN calls_analyzed c
ON l.loan_id=c.loan_id

LEFT JOIN agents_enriched a
ON a.agent_id=c.agent_id

LEFT JOIN agents_enriched ag
ON ag.agent_id=a.agent_id

GROUP BY

agent_id,
loan_id,
borrower_name,
dpd_days,
overdue_amount,
loan_aging_bucket,
l.loan_status;

/* ===========================================================
 3. MANAGER BRANCH SUMMARY
=========================================================== */

CREATE TABLE IF NOT EXISTS manager_branch_summary
(
    branch_id String,
    report_date Date,
    agents_active_today UInt64,
    total_calls_made UInt64,
    calls_successful UInt64,
    collection_today Float64,
    collection_target Float64,
    target_achievement_pct Float64,
    avg_call_duration Float64,
    followups_pending UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (branch_id, report_date);



CREATE MATERIALIZED VIEW IF NOT EXISTS mv_manager_branch_summary
TO manager_branch_summary
AS
SELECT

    ifNull(nullIf(ag.branch_id,''),'unknown')
        AS branch_id,

    today() AS report_date,

    uniqIf(
        ag.agent_id,
        toDate(c.call_start_time)=today()
    ) AS agents_active_today,

    countIf(
        c.call_id,
        toDate(c.call_start_time)=today()
    ) AS total_calls_made,

    countIf(

        c.call_id,

        toDate(c.call_start_time)=today()

        AND

        ifNull(c.call_success_flag,0)=1

    ) AS calls_successful,

    sumIf(

        ifNull(p.amount,0),

        toDate(p.payment_date)=today()

    ) AS collection_today,

    0.0 AS collection_target,

    0.0 AS target_achievement_pct,

    avgIf(

        ifNull(c.call_duration_sec,0),

        toDate(c.call_start_time)=today()

    ) AS avg_call_duration,

    sumIf(

        1,

        toDate(c.call_start_time)
            < today()-1

        AND

        upper(ifNull(l.loan_status,''))='ACTIVE'

    ) AS followups_pending

FROM calls_analyzed c

LEFT JOIN loans_clean l
ON c.loan_id=l.loan_id

LEFT JOIN agents_enriched ag
ON c.agent_id=ag.agent_id

LEFT JOIN payments_clean p
ON l.loan_id=p.loan_id

GROUP BY branch_id;



/* ===========================================================
 4. HR AGENT PERFORMANCE DAILY
=========================================================== */

CREATE TABLE IF NOT EXISTS hr_agent_performance_daily
(
    agent_id String,
    report_date Date,
    total_calls UInt64,
    calls_completed UInt64,
    call_success_rate Float64,
    total_talk_time_min Float64,
    avg_call_duration_sec Float64,
    amount_collected Float64,
    loans_resolved UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (agent_id, report_date);



CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hr_agent_performance_daily
TO hr_agent_performance_daily
AS
SELECT

    c.agent_id,

    toDate(c.call_start_time)
        AS report_date,

    count() AS total_calls,

    countIf(

        c.call_status='COMPLETED'

    ) AS calls_completed,

    if(

        count()=0,

        0,

        countIf(
            c.call_status='COMPLETED'
        )/count()*100

    ) AS call_success_rate,

    sum(ifNull(c.call_duration_sec,0))
        /60.0 AS total_talk_time_min,

    avgIf(

        ifNull(c.call_duration_sec,0),

        ifNull(c.call_duration_sec,0)>0

    ) AS avg_call_duration_sec,

    sumIf(

        ifNull(p.amount,0),

        toDate(p.payment_date)
        =
        toDate(c.call_start_time)

    ) AS amount_collected,

    countIf(

        l.loan_status='CLOSED'

    ) AS loans_resolved

FROM calls_analyzed c

LEFT JOIN payments_clean p
ON c.loan_id=p.loan_id

LEFT JOIN loans_clean l
ON c.loan_id=l.loan_id

GROUP BY
    c.agent_id,
    report_date;


INSERT INTO lender_portfolio_summary

SELECT

'unknown' AS lender_id,

l.loan_aging_bucket AS loan_aging_bucket,

count() AS total_loans_count,

sum(ifNull(l.principal_amount,0))
AS total_principal_disbursed,

sum(

if(

ifNull(l.npa_flag,0)=1,

1,

0

)

)

AS npa_count,

sum(

if(

ifNull(l.npa_flag,0)=1,

ifNull(l.principal_amount,0),

0

)

)

AS npa_amount,

sum(ifNull(p.amount,0))
AS total_collected_today,

if(

sum(ifNull(l.principal_amount,0))=0,

0,

sum(ifNull(l.total_paid,0))

/

sum(ifNull(l.principal_amount,0))*100

)

AS collection_efficiency_pct

FROM loans_clean l

LEFT JOIN payments_clean p

ON l.loan_id=p.loan_id

GROUP BY

l.loan_aging_bucket;
INSERT INTO manager_branch_summary

SELECT

    ifNull(nullIf(ag.branch_id,''),'unknown') AS branch_id,

    today() AS report_date,

    uniqIf(
        ag.agent_id,
        toDate(c.call_start_time)=today()
    ) AS agents_active_today,

    countIf(
        c.call_id,
        toDate(c.call_start_time)=today()
    ) AS total_calls_made,

    countIf(

        c.call_id,

        toDate(c.call_start_time)=today()

        AND

        ifNull(c.call_success_flag,0)=1

    ) AS calls_successful,

    sumIf(

        ifNull(p.amount,0),

        toDate(p.payment_date)=today()

    ) AS collection_today,

    0.0 AS collection_target,

    0.0 AS target_achievement_pct,

    avgIf(

        ifNull(c.call_duration_sec,0),

        toDate(c.call_start_time)=today()

    ) AS avg_call_duration,

    sumIf(

        1,

        toDate(c.call_start_time)
            < today()-1

        AND

        upper(ifNull(l.loan_status,''))='ACTIVE'

    ) AS followups_pending

FROM calls_analyzed c

LEFT JOIN loans_clean l
ON c.loan_id=l.loan_id

LEFT JOIN agents_enriched ag
ON c.agent_id=ag.agent_id

LEFT JOIN payments_clean p
ON l.loan_id=p.loan_id

GROUP BY branch_id;


INSERT INTO agent_assigned_loans

SELECT

ifNull(c.agent_id,'unassigned') AS agent_id,

l.loan_id,

ifNull(l.borrower_id,'') AS borrower_name,

toInt32(ifNull(l.dpd_days,0)),

ifNull(l.overdue_amount,0),

l.loan_aging_bucket,

max(ifNull(c.call_start_time,
toDateTime('1970-01-01 00:00:00'))) AS last_call_at,

any(ifNull(c.call_status,'')),

toUInt32(any(ifNull(c.call_duration_sec,0))),

toUInt8(

(max(ifNull(c.call_start_time,
toDateTime('1970-01-01 00:00:00')))
< now()-INTERVAL 1 DAY)

AND

upper(ifNull(l.loan_status,''))='ACTIVE'

),

countIf(

toDate(c.call_start_time)=today()

)

FROM loans_clean l

LEFT JOIN calls_analyzed c
ON l.borrower_id = c.loan_id

GROUP BY

c.agent_id,
l.loan_id,
l.borrower_id,
l.dpd_days,
l.overdue_amount,
l.loan_aging_bucket,
l.loan_status;
INSERT INTO manager_branch_summary

SELECT

ifNull(nullIf(ag.branch_id,''),'unknown')
AS branch_id,

today()
AS report_date,

uniqIf(

ag.agent_id,

toDate(c.call_start_time)=today()

)

AS agents_active_today,

countIf(

c.call_id,

toDate(c.call_start_time)=today()

)

AS total_calls_made,

countIf(

c.call_id,

toDate(c.call_start_time)=today()

AND

ifNull(c.call_success_flag,0)=1

)

AS calls_successful,

sumIf(

ifNull(p.amount,0),

toDate(p.payment_date)=today()

)

AS collection_today,

0.0,

0.0,

avgIf(

ifNull(c.call_duration_sec,0),

toDate(c.call_start_time)=today()

)

AS avg_call_duration,

sumIf(

1,

toDate(c.call_start_time)
< today()-1

AND

upper(ifNull(l.loan_status,''))='ACTIVE'

)

AS followups_pending

FROM calls_analyzed c

LEFT JOIN loans_clean l
ON c.loan_id=l.loan_id

LEFT JOIN agents_enriched ag
ON c.agent_id=ag.agent_id

LEFT JOIN payments_clean p
ON l.loan_id=p.loan_id

GROUP BY branch_id;
INSERT INTO hr_agent_performance_daily

SELECT

c.agent_id
AS agent_id,

toDate(c.call_start_time)
AS report_date,

count()
AS total_calls,

countIf(
c.call_status='COMPLETED'
)

AS calls_completed,

if(

count()=0,

0,

countIf(

c.call_status='COMPLETED'

)/count()*100

)

AS call_success_rate,

sum(ifNull(c.call_duration_sec,0))
/60.0

AS total_talk_time_min,

avgIf(

ifNull(c.call_duration_sec,0),

ifNull(c.call_duration_sec,0)>0

)

AS avg_call_duration_sec,

sumIf(

ifNull(p.amount,0),

toDate(p.payment_date)
=
toDate(c.call_start_time)

)

AS amount_collected,

countIf(

l.loan_status='CLOSED'

)

AS loans_resolved

FROM calls_analyzed c

LEFT JOIN payments_clean p
ON c.loan_id=p.loan_id

LEFT JOIN loans_clean l
ON c.loan_id=l.loan_id

GROUP BY

agent_id,
report_date;
