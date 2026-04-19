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
-- Note: Password should be set via environment variable DATA_PIPELINE_DB_PASSWORD
CREATE USER IF NOT EXISTS immich_readonly WITH PASSWORD 'readonly_password';

-- Grant CONNECT privilege to the database
GRANT CONNECT ON DATABASE aesthetic_hub TO immich_readonly;

-- Grant USAGE on the public schema (required to access tables)
GRANT USAGE ON SCHEMA public TO immich_readonly;

-- Grant SELECT permission on aesthetic_scores table only (if it exists)
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'aesthetic_scores') THEN
        GRANT SELECT ON TABLE aesthetic_scores TO immich_readonly;
        REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE aesthetic_scores FROM immich_readonly;
    END IF;
END $$;

-- Note: Verification commands (\du, \dp) are commented out for docker-entrypoint-initdb.d
-- You can verify manually by connecting to the database after startup
