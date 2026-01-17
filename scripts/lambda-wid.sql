--jinja
-- =============================================================================
-- 🔑 Snowflake WIDF Setup for AWS Lambda
-- =============================================================================
-- This creates a SERVICE user that authenticates via Lambda's IAM role.
-- NO passwords, NO secrets - just IAM trust!
--
-- Variables passed from Taskfile:
--   <%snowflake_user%>     - Service user name (e.g., LAMBDA_LOADER_BOT)
--   <%aws_role_arn%>       - Full IAM role ARN
--   <%snowflake_role%>     - Role to grant to service user
--   <%snowflake_warehouse%> - Default warehouse
--   <%demo_database%>      - Database name
--   <%demo_schema%>        - Schema name
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- Create WIDF Service User
-- =============================================================================
-- 🔑 THE MAGIC: This user trusts the Lambda's IAM role ARN

SELECT '🔑 Creating WIDF user with trusted ARN: <%aws_role_arn%>' AS info;

CREATE USER IF NOT EXISTS <%snowflake_user%>
    WORKLOAD_IDENTITY = (
        TYPE = AWS
        ARN = '<%aws_role_arn%>'
    )
    TYPE = SERVICE
    DEFAULT_ROLE = <%snowflake_role%>
    DEFAULT_WAREHOUSE = <%snowflake_warehouse%>
    COMMENT = '🔑 WIDF service user for AWS Lambda - keyless authentication!';

-- Grant role to service user
GRANT ROLE <%snowflake_role%> TO USER <%snowflake_user%>;

-- Grant warehouse usage to the role
GRANT USAGE ON WAREHOUSE <%snowflake_warehouse%> TO ROLE <%snowflake_role%>;

-- Grant database and schema access
GRANT USAGE ON DATABASE <%demo_database%> TO ROLE <%snowflake_role%>;
GRANT USAGE ON SCHEMA <%demo_database%>.<%demo_schema%> TO ROLE <%snowflake_role%>;

-- Grant table privileges (for RAW_DATA table)
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA <%demo_database%>.<%demo_schema%> TO ROLE <%snowflake_role%>;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA <%demo_database%>.<%demo_schema%> TO ROLE <%snowflake_role%>;

-- Grant CREATE TABLE for auto-creating staging tables
GRANT CREATE TABLE ON SCHEMA <%demo_database%>.<%demo_schema%> TO ROLE <%snowflake_role%>;

-- =============================================================================
-- Verify
-- =============================================================================

DESCRIBE USER <%snowflake_user%>;
SHOW USER WORKLOAD IDENTITY AUTHENTICATION METHODS FOR USER <%snowflake_user%>;

SELECT 
    '✅ WIDF user created!' AS status,
    '<%snowflake_user%>' AS service_user,
    '<%aws_role_arn%>' AS trusted_role_arn;
