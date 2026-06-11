-- ============================================================
-- Snowflake setup for the banking pipeline (run once, in a Snowflake worksheet)
-- Creates the warehouse, BANKING database, RAW + ANALYTICS schemas,
-- and the RAW landing tables (one VARIANT column `v` per table — the
-- Airflow COPY INTO loads each parquet row as a single VARIANT).
-- ============================================================

-- Use an admin role (free trial defaults to ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;

-- Warehouse (free trial usually already has COMPUTE_WH; this is idempotent)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database + schemas
CREATE DATABASE IF NOT EXISTS BANKING;
CREATE SCHEMA IF NOT EXISTS BANKING.RAW;        -- Bronze: raw CDC parquet
CREATE SCHEMA IF NOT EXISTS BANKING.ANALYTICS;  -- Silver/Gold: dbt staging, snapshots, marts

-- RAW landing tables: a single VARIANT column `v` per source table.
-- dbt staging models read these as v:col::type.
USE SCHEMA BANKING.RAW;

CREATE TABLE IF NOT EXISTS RAW.customers         (v VARIANT);
CREATE TABLE IF NOT EXISTS RAW.accounts          (v VARIANT);
CREATE TABLE IF NOT EXISTS RAW.transactions      (v VARIANT);
CREATE TABLE IF NOT EXISTS RAW.services          (v VARIANT);
CREATE TABLE IF NOT EXISTS RAW.service_usage     (v VARIANT);
-- Customer 360 flow
CREATE TABLE IF NOT EXISTS RAW.customer_profiles (v VARIANT);
CREATE TABLE IF NOT EXISTS RAW.customer_segments (v VARIANT);

-- Quick check
SHOW TABLES IN SCHEMA BANKING.RAW;
