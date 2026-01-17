--jinja
-- =============================================================================
-- 🧹 Cleanup Snowflake WIDF Demo Resources
-- =============================================================================
-- This script removes all resources created for the WIDF Lambda demo.
-- Run this when you want to completely clean up after the demo.
--
-- Variables passed from Taskfile:
--   <%snowflake_user%>     - WIDF service user to drop
--   <%snowflake_role%>     - Demo role to drop
--   <%demo_database%>      - Database to drop (includes all schemas/tables)
--   <%snowflake_warehouse%> - Warehouse (NOT dropped - just revokes)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- Show what will be cleaned up
-- =============================================================================

SELECT '🧹 Cleaning up WIDF Demo resources...' AS info;

SELECT 
    '<%snowflake_user%>' AS service_user_to_drop,
    '<%snowflake_role%>' AS role_to_drop,
    '<%demo_database%>' AS database_to_drop;

-- =============================================================================
-- Revoke grants first (prevents dependency errors)
-- =============================================================================

-- Revoke warehouse usage from role
REVOKE USAGE ON WAREHOUSE <%snowflake_warehouse%> FROM ROLE <%snowflake_role%>;

-- Revoke role from BOT user
REVOKE ROLE <%snowflake_role%> FROM USER <%snowflake_user%>;
-- Reovoke role from current Snowflake user
set current_user = (SELECT CURRENT_USER());
REVOKE ROLE <%sa_role%> FROM USER IDENTIFIER($current_user);

-- =============================================================================
-- Drop WIDF Service User
-- =============================================================================

DROP USER IF EXISTS <%snowflake_user%>;
SELECT '✅ Dropped user: <%snowflake_user%>' AS status;

-- =============================================================================
-- Drop Demo Database (cascades to schema and tables)
-- =============================================================================

DROP DATABASE IF EXISTS <%demo_database%>;
SELECT '✅ Dropped database: <%demo_database%>' AS status;

-- =============================================================================
-- Drop Demo Role
-- =============================================================================

DROP ROLE IF EXISTS <%snowflake_role%>;
SELECT '✅ Dropped role: <%snowflake_role%>' AS status;

-- =============================================================================
-- Verify cleanup
-- =============================================================================

SELECT '🧹 Cleanup complete!' AS status;

-- These should return empty or "does not exist" errors (which is expected)
-- DESCRIBE USER <%snowflake_user%>;
-- DESCRIBE DATABASE <%demo_database%>;
-- SHOW GRANTS TO ROLE <%snowflake_role%>;
