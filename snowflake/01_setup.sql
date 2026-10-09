-- Run in a Snowsight worksheet, section by section, in order.
-- Replace <BUCKET> and <ROLE_ARN> before running.

-- ===== Section 1: database, schema, integration (needs ACCOUNTADMIN) =====
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;  -- the default trial warehouse; change if yours is named differently

CREATE DATABASE IF NOT EXISTS BANKING;
CREATE SCHEMA IF NOT EXISTS BANKING.RAW;

-- Lets Snowflake read S3 by assuming an AWS IAM role, so no keys are stored in Snowflake.
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<ROLE_ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/raw/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this output into the AWS role trust policy.
DESC INTEGRATION s3_int;

-- ===== Section 2: stage and raw tables (run after the AWS trust policy is updated) =====
CREATE STAGE IF NOT EXISTS BANKING.RAW.s3_stage
  URL = 's3://<BUCKET>/raw/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (TYPE = JSON);

LIST @BANKING.RAW.s3_stage;  -- should list your .jsonl files

-- One VARIANT column holds the whole Debezium event untouched; dbt parses it later.
CREATE TABLE IF NOT EXISTS BANKING.RAW.CUSTOMERS    (event VARIANT, source_file STRING, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS BANKING.RAW.ACCOUNTS     (event VARIANT, source_file STRING, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS BANKING.RAW.TRANSACTIONS (event VARIANT, source_file STRING, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP);

-- ===== Section 3: load (safe to re-run: Snowflake remembers which files it already loaded) =====
COPY INTO BANKING.RAW.CUSTOMERS (event, source_file)
  FROM (SELECT $1, METADATA$FILENAME FROM @BANKING.RAW.s3_stage/customers/);
COPY INTO BANKING.RAW.ACCOUNTS (event, source_file)
  FROM (SELECT $1, METADATA$FILENAME FROM @BANKING.RAW.s3_stage/accounts/);
COPY INTO BANKING.RAW.TRANSACTIONS (event, source_file)
  FROM (SELECT $1, METADATA$FILENAME FROM @BANKING.RAW.s3_stage/transactions/);

-- ===== Section 4: check =====
SELECT COUNT(*) FROM BANKING.RAW.TRANSACTIONS;

SELECT event:after.id::INT              AS id,
       event:after.balance::NUMBER(12,2) AS balance,
       event:op::STRING                  AS op
FROM BANKING.RAW.ACCOUNTS
LIMIT 10;
