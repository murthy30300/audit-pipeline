-- PostgreSQL Schema for Compliance Database
-- Includes updated_at trigger for Airflow watermark queries

-- ============================================================================
-- Trigger Function for updated_at
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Loans Table (Silver)
-- ============================================================================
CREATE TABLE IF NOT EXISTS loans_clean (
  loan_id UUID PRIMARY KEY,
  borrower_id UUID NOT NULL,
  principal_amount NUMERIC(15,2),
  emi_amount NUMERIC(15,2),
  tenure_months INTEGER DEFAULT 12,
  loan_status VARCHAR(50),
  due_date DATE,
  disbursement_date DATE,
  total_paid NUMERIC(15,2),
  dpd_days INTEGER DEFAULT 0,
  loan_aging_bucket VARCHAR(50),
  npa_flag BOOLEAN DEFAULT FALSE,
  overdue_amount NUMERIC(15,2),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TRIGGER set_loans_updated_at
BEFORE UPDATE ON loans_clean
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- Calls Table (Silver)
-- ============================================================================
CREATE TABLE IF NOT EXISTS calls_analyzed (
  call_id UUID PRIMARY KEY,
  loan_id UUID NOT NULL,
  agent_id UUID,
  call_start_time TIMESTAMP,
  call_duration_sec INTEGER,
  call_status VARCHAR(50),
  transcript TEXT,
  call_success_flag BOOLEAN DEFAULT FALSE,
  call_hour INTEGER,
  is_within_hours BOOLEAN DEFAULT FALSE,
  contact_attempt_seq INTEGER,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TRIGGER set_calls_updated_at
BEFORE UPDATE ON calls_analyzed
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- Payments Table (Silver)
-- ============================================================================
CREATE TABLE IF NOT EXISTS payments_clean (
  payment_id UUID PRIMARY KEY,
  loan_id UUID NOT NULL,
  customer_id UUID,
  amount NUMERIC(15,2),
  emi_amount NUMERIC(15,2),
  payment_date DATE,
  payment_status VARCHAR(50),
  bounce_flag BOOLEAN DEFAULT FALSE,
  partial_pay_flag BOOLEAN DEFAULT FALSE,
  collection_efficiency NUMERIC(5,2),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TRIGGER set_payments_updated_at
BEFORE UPDATE ON payments_clean
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- Messages/SMS Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS messages_processed (
  message_id UUID PRIMARY KEY,
  loan_id UUID NOT NULL,
  customer_id UUID,
  message_type VARCHAR(50),
  message_text TEXT,
  sent_at TIMESTAMP,
  delivery_status VARCHAR(50),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TRIGGER set_messages_updated_at
BEFORE UPDATE ON messages_processed
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- CRM Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm_interactions (
  crm_id UUID PRIMARY KEY,
  loan_id UUID NOT NULL,
  customer_id UUID,
  interaction_type VARCHAR(50),
  interaction_date TIMESTAMP,
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TRIGGER set_crm_updated_at
BEFORE UPDATE ON crm_interactions
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- Indexes for Performance
-- ============================================================================
CREATE INDEX idx_loans_borrower_id ON loans_clean(borrower_id);
CREATE INDEX idx_loans_status ON loans_clean(loan_status);
CREATE INDEX idx_loans_updated_at ON loans_clean(updated_at);

CREATE INDEX idx_calls_loan_id ON calls_analyzed(loan_id);
CREATE INDEX idx_calls_start_time ON calls_analyzed(call_start_time);
CREATE INDEX idx_calls_updated_at ON calls_analyzed(updated_at);

CREATE INDEX idx_payments_loan_id ON payments_clean(loan_id);
CREATE INDEX idx_payments_date ON payments_clean(payment_date);
CREATE INDEX idx_payments_updated_at ON payments_clean(updated_at);

CREATE INDEX idx_messages_loan_id ON messages_processed(loan_id);
CREATE INDEX idx_messages_updated_at ON messages_processed(updated_at);

CREATE INDEX idx_crm_loan_id ON crm_interactions(loan_id);
CREATE INDEX idx_crm_updated_at ON crm_interactions(updated_at);
