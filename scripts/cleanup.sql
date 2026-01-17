--jinja
-- Cleanup Snowflake WIDF Demo Resources
-- Removes user, role, and database created for the demo
--
-- Variables:
--   <%snowflake_user%>      - WIDF service user to drop
--   <%snowflake_role%>      - Demo role to drop
--   <%sa_role%>             - SA role to revoke from current user
--   <%demo_database%>       - Database to drop
--   <%snowflake_warehouse%> - Warehouse (revokes only, not dropped)

USE ROLE ACCOUNTADMIN;

SELECT 'Cleaning up WIDF Demo resources' AS info;

SELECT 
    '<%snowflake_user%>' AS service_user_to_drop,
    '<%snowflake_role%>' AS role_to_drop,
    '<%demo_database%>' AS database_to_drop;

-- Revoke grants first
REVOKE USAGE ON WAREHOUSE <%snowflake_warehouse%> FROM ROLE <%snowflake_role%>;
REVOKE ROLE <%snowflake_role%> FROM USER <%snowflake_user%>;

SET current_user = (SELECT CURRENT_USER());
REVOKE ROLE <%sa_role%> FROM USER IDENTIFIER($current_user);

-- Drop WIDF Service User
DROP USER IF EXISTS <%snowflake_user%>;
SELECT 'Dropped user: <%snowflake_user%>' AS status;

-- Drop Demo Database (cascades to schema and tables)
DROP DATABASE IF EXISTS <%demo_database%>;
SELECT 'Dropped database: <%demo_database%>' AS status;

-- Drop Demo Role
DROP ROLE IF EXISTS <%snowflake_role%>;
SELECT 'Dropped role: <%snowflake_role%>' AS status;

SELECT 'Cleanup complete' AS status;
