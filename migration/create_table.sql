CREATE Table IF NOT EXISTS customers (
  id Serial PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE default NOW()
);

CREATE TABLE IF NOT EXISTS ACCOUNTS(
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL references customers(id) ON DELETE CASCADE,
  account_type VARCHAR(50) NOT NULL,
  balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS transactions(
  id BIGSERIAL PRIMARY KEY,
  account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  txn_type VARCHAR(50) NOT NULL,
  amount numeric(18,2) NOT NULL CHECK (amount > 0),
  related_account_id INT NULL,
  status varchar(20) NOT NULL DEFAULT 'COMPLETED',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Catalog of banking services (dimension)
CREATE TABLE IF NOT EXISTS services(
  id SERIAL PRIMARY KEY,
  service_code VARCHAR(50) UNIQUE NOT NULL,
  service_name VARCHAR(100) NOT NULL,
  category VARCHAR(50) NOT NULL, -- PAYMENTS | TRANSFERS | INQUIRY | LOANS | CARDS | ACCOUNT
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- One row per service-usage event (fact source for "service usage frequency")
CREATE TABLE IF NOT EXISTS service_usage(
  id BIGSERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  account_id INT NULL REFERENCES accounts(id) ON DELETE SET NULL,
  service_id INT NOT NULL REFERENCES services(id),
  channel VARCHAR(20) NOT NULL, -- WEB | MOBILE | ATM | BRANCH | CALL_CENTER
  status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS', -- SUCCESS | FAILED
  used_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_service_usage_service_used ON service_usage(service_id, used_at);
CREATE INDEX IF NOT EXISTS idx_service_usage_customer_used ON service_usage(customer_id, used_at);

-- ============================================================================
-- Customer 360 / Segmentation flow (sources for customer-behavior analytics)
-- ============================================================================

-- Aggregated analytical profile per customer (Customer 360 view)
CREATE TABLE IF NOT EXISTS customer_profiles (
  customer_id INT PRIMARY KEY REFERENCES customers(id) ON DELETE CASCADE,
  total_transactions INT DEFAULT 0,
  total_transfer_amount NUMERIC(18,2) DEFAULT 0,
  avg_transaction_amount NUMERIC(18,2) DEFAULT 0,
  preferred_transaction_type VARCHAR(50),
  login_frequency INT DEFAULT 0,
  favorite_feature VARCHAR(100),
  last_active_date TIMESTAMP WITH TIME ZONE,
  risk_score NUMERIC(5,2) DEFAULT 0 CHECK (risk_score >= 0 AND risk_score <= 100),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Rule-based segment assignment per customer
CREATE TABLE IF NOT EXISTS customer_segments (
  customer_id INT PRIMARY KEY REFERENCES customers(id) ON DELETE CASCADE,
  segment_name VARCHAR(50) NOT NULL, -- VIP | Active User | Saver | Dormant User | Risky User | ...
  segment_score NUMERIC(5,2) DEFAULT 0,
  assigned_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_profiles_risk ON customer_profiles(risk_score);
CREATE INDEX IF NOT EXISTS idx_customer_segments_name ON customer_segments(segment_name);
