-- Initialize aesthetic_scores table for Data Pipeline Database
-- This script creates the table structure needed for storing aesthetic scores

-- Create aesthetic_scores table
CREATE TABLE IF NOT EXISTS aesthetic_scores (
    id SERIAL PRIMARY KEY,
    asset_id UUID NOT NULL,
    user_id UUID NOT NULL,
    score FLOAT NOT NULL CHECK (score >= 0 AND score <= 1),
    global_score FLOAT,
    model_version VARCHAR(50),
    is_cold_start BOOLEAN DEFAULT TRUE,
    alpha FLOAT DEFAULT 0.0,
    inference_request_id VARCHAR(255),
    source VARCHAR(100) DEFAULT 'scoring-service',
    scored_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, user_id)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_aesthetic_scores_asset_id ON aesthetic_scores(asset_id);
CREATE INDEX IF NOT EXISTS idx_aesthetic_scores_user_id ON aesthetic_scores(user_id);
CREATE INDEX IF NOT EXISTS idx_aesthetic_scores_score ON aesthetic_scores(score DESC);
CREATE INDEX IF NOT EXISTS idx_aesthetic_scores_created_at ON aesthetic_scores(created_at DESC);

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_aesthetic_scores_updated_at 
    BEFORE UPDATE ON aesthetic_scores 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions to the main user
GRANT ALL PRIVILEGES ON TABLE aesthetic_scores TO aesthetic;
GRANT USAGE, SELECT ON SEQUENCE aesthetic_scores_id_seq TO aesthetic;

-- Create readonly user if it doesn't exist and grant permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'immich_readonly') THEN
        CREATE USER immich_readonly WITH PASSWORD 'readonly_password';
    END IF;
END $$;

-- Grant permissions to readonly user
GRANT CONNECT ON DATABASE aesthetic_hub TO immich_readonly;
GRANT USAGE ON SCHEMA public TO immich_readonly;
GRANT SELECT ON TABLE aesthetic_scores TO immich_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE aesthetic_scores FROM immich_readonly;
