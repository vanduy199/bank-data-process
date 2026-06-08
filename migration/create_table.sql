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
