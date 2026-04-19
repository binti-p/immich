-- ============================================================================
-- Create Read-Only Database User for Immich Server
-- ============================================================================
-- This script creates a read-only PostgreSQL user for Immich Server to query
-- aesthetic scores from the Data Pipeline database.
--
-- User: immich_readonly
-- Permissions: SELECT only on aesthetic_scores table
-- Database: aesthetic_hub (Data Pipeline PostgreSQL)
--
-- Requirements: 16.2 (Security and Access Control)
-- ============================================================================

-- Create the read-only user
-- Note: Replace 'CHANGE_ME_PASSWORD' with a strong password
CREATE USER immich_readonly WITH PASSWORD 'CHANGE_ME_PASSWORD';

-- Grant CONNECT privilege to the database
GRANT CONNECT ON DATABASE aesthetic_hub TO immich_readonly;

-- Connect to the aesthetic_hub database to grant table-level permissions
\c aesthetic_hub

-- Grant USAGE on the public schema (required to access tables)
GRANT USAGE ON SCHEMA public TO immich_readonly;

-- Grant SELECT permission on aesthetic_scores table only
GRANT SELECT ON TABLE aesthetic_scores TO immich_readonly;

-- Revoke all other permissions to ensure read-only access
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE aesthetic_scores FROM immich_readonly;

-- Verify the user has been created and permissions are correct
\du immich_readonly
\dp aesthetic_scores

-- ============================================================================
-- Expected Output:
-- - User 'immich_readonly' should exist
-- - aesthetic_scores table should show SELECT permission for immich_readonly
-- ============================================================================
