-- Gold layer materialized views definitions

-- mv_lender_portfolio_summary
-- Aggregates loans_clean + payments_clean by lender_id and loan_aging_bucket
CREATE TABLE IF NOT EXISTS lender_portfolio_summary (
	lender_id String,
	loan_aging_bucket String,
	total_loans_count UInt64 DEFAULT 0,
	total_principal_disbursed Float64 DEFAULT 0,
	npa_count UInt64 DEFAULT 0,
	npa_amount Float64 DEFAULT 0,
	total_collected_today Float64 DEFAULT 0,
	collection_efficiency_pct Float64 DEFAULT 0,
	bucket_breakdown Array(Tuple(String, UInt64, Float64))
) ENGINE = SummingMergeTree()
ORDER BY (lender_id, loan_aging_bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_lender_portfolio_summary
TO lender_portfolio_summary AS
SELECT
	coalesce(l.lender_id, 'unknown') AS lender_id,
	l.loan_aging_bucket,
	count(l.loan_id) AS total_loans_count,
	sum(l.principal_amount) AS total_principal_disbursed,
	sum(if(l.npa_flag, 1, 0)) AS npa_count,
	sum(if(l.npa_flag, l.principal_amount, 0.0)) AS npa_amount,
	sumIf(p.amount, toDate(p.payment_date) = today()) AS total_collected_today,
	(sum(l.total_paid) / nullIf(sum(l.principal_amount), 0)) * 100 AS collection_efficiency_pct,
	groupArray((l.loan_aging_bucket, count(l.loan_id), sum(l.principal_amount))) AS bucket_breakdown
FROM loans_clean AS l
LEFT JOIN payments_clean AS p ON l.loan_id = p.loan_id
GROUP BY lender_id, l.loan_aging_bucket;


-- mv_agent_assigned_loans
-- Joins loans_clean + calls_analyzed + agents_enriched to provide per-agent loan view
CREATE TABLE IF NOT EXISTS agent_assigned_loans (
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
) ENGINE = SummingMergeTree()
ORDER BY (agent_id, loan_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agent_assigned_loans
TO agent_assigned_loans AS
SELECT
	coalesce(a.agent_id, 'unassigned') AS agent_id,
	l.loan_id,
	coalesce(ag.name, '') AS borrower_name,
	l.dpd_days,
	l.overdue_amount,
	l.loan_aging_bucket,
	max(c.call_start_time) AS last_call_at,
	any(c.call_status) AS last_call_status,
	any(c.call_duration_sec) AS last_call_duration,
	toUInt8((max(c.call_start_time) < now() - INTERVAL 1 DAY) AND (upper(l.loan_status) = 'ACTIVE')) AS followup_due,
	countIf(c.call_id, toDate(c.call_start_time) = today()) AS calls_today
FROM loans_clean AS l
LEFT JOIN calls_analyzed AS c ON l.loan_id = c.loan_id
LEFT JOIN agents_enriched AS a ON a.agent_id = c.agent_id
LEFT JOIN agents_enriched AS ag ON ag.agent_id = a.agent_id
GROUP BY agent_id, l.loan_id, ag.name, l.dpd_days, l.overdue_amount, l.loan_aging_bucket, l.loan_status;


-- mv_manager_branch_summary
-- Aggregates branch-level metrics daily
CREATE TABLE IF NOT EXISTS manager_branch_summary (
	branch_id String,
	report_date Date,
	agents_active_today UInt64 DEFAULT 0,
	total_calls_made UInt64 DEFAULT 0,
	calls_successful UInt64 DEFAULT 0,
	collection_today Float64 DEFAULT 0,
	collection_target Float64 DEFAULT 0,
	target_achievement_pct Float64 DEFAULT 0,
	avg_call_duration Float64 DEFAULT 0,
	followups_pending UInt64 DEFAULT 0
) ENGINE = SummingMergeTree()
ORDER BY (branch_id, report_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_manager_branch_summary
TO manager_branch_summary AS
SELECT
	coalesce(ag.branch_id, 'unknown') AS branch_id,
	today() AS report_date,
	uniqIf(ag.agent_id, toDate(c.call_start_time) = today()) AS agents_active_today,
	countIf(c.call_id, toDate(c.call_start_time) = today()) AS total_calls_made,
	countIf(c.call_id, toDate(c.call_start_time) = today() AND c.call_success_flag) AS calls_successful,
	sumIf(p.amount, toDate(p.payment_date) = today()) AS collection_today,
	0.0 AS collection_target,
	0.0 AS target_achievement_pct,
	avgIf(c.call_duration_sec, toDate(c.call_start_time) = today()) AS avg_call_duration,
	sumIf((toDate(c.call_start_time) < today() - 1) AND (upper(l.loan_status) = 'ACTIVE'), 1) AS followups_pending
FROM calls_analyzed AS c
LEFT JOIN loans_clean AS l ON c.loan_id = l.loan_id
LEFT JOIN agents_enriched AS ag ON c.agent_id = ag.agent_id
LEFT JOIN payments_clean AS p ON l.loan_id = p.loan_id
GROUP BY branch_id;


-- mv_hr_agent_performance_daily
CREATE TABLE IF NOT EXISTS hr_agent_performance_daily (
	agent_id String,
	report_date Date,
	total_calls UInt64 DEFAULT 0,
	calls_completed UInt64 DEFAULT 0,
	call_success_rate Float64 DEFAULT 0,
	total_talk_time_min Float64 DEFAULT 0,
	avg_call_duration_sec Float64 DEFAULT 0,
	amount_collected Float64 DEFAULT 0,
	loans_resolved UInt64 DEFAULT 0
) ENGINE = SummingMergeTree()
ORDER BY (agent_id, report_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hr_agent_performance_daily
TO hr_agent_performance_daily AS
SELECT
	c.agent_id,
	toDate(c.call_start_time) AS report_date,
	count(c.call_id) AS total_calls,
	countIf(c.call_id, c.call_status = 'COMPLETED') AS calls_completed,
	(countIf(c.call_id, c.call_status = 'COMPLETED') / nullIf(count(c.call_id), 0)) * 100 AS call_success_rate,
	sum(c.call_duration_sec) / 60.0 AS total_talk_time_min,
	avgIf(c.call_duration_sec, c.call_duration_sec > 0) AS avg_call_duration_sec,
	sumIf(p.amount, toDate(p.payment_date) = toDate(c.call_start_time)) AS amount_collected,
	countIf(l.loan_id, l.loan_status = 'CLOSED') AS loans_resolved
FROM calls_analyzed AS c
LEFT JOIN payments_clean AS p ON c.loan_id = p.loan_id
LEFT JOIN loans_clean AS l ON c.loan_id = l.loan_id
GROUP BY c.agent_id, toDate(c.call_start_time);
