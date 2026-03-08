CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw_data.user_sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) UNIQUE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device_type VARCHAR(50),
    device_info JSONB,
    actions TEXT[],
    session_duration_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.event_logs (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    details JSONB,
    user_id VARCHAR(100),
    session_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.support_tickets (
    id SERIAL PRIMARY KEY,
    ticket_id VARCHAR(100) UNIQUE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100),
    messages JSONB,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    first_response_time TIMESTAMP,
    resolution_time_hours NUMERIC(10,2),
    created_at_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.user_recommendations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) UNIQUE NOT NULL,
    recommended_products TEXT[],
    last_updated TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.moderation_queue (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) UNIQUE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    review_text TEXT,
    rating INTEGER,
    moderation_status VARCHAR(50) NOT NULL,
    flags TEXT[],
    submitted_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.replication_state (
    id SERIAL PRIMARY KEY,
    collection_name VARCHAR(100) NOT NULL UNIQUE,
    last_sequence_id VARCHAR(255),
    last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_synced INTEGER DEFAULT 0
);

CREATE INDEX idx_user_sessions_user_id ON raw_data.user_sessions(user_id);
CREATE INDEX idx_user_sessions_start_time ON raw_data.user_sessions(start_time);
CREATE INDEX idx_event_logs_timestamp ON raw_data.event_logs(timestamp);
CREATE INDEX idx_support_tickets_user_id ON raw_data.support_tickets(user_id);
CREATE INDEX idx_support_tickets_status ON raw_data.support_tickets(status);
CREATE INDEX idx_moderation_queue_status ON raw_data.moderation_queue(moderation_status);

INSERT INTO raw_data.replication_state (collection_name, last_sequence_id) VALUES
    ('user_sessions', '0'),
    ('event_logs', '0'),
    ('support_tickets', '0'),
    ('user_recommendations', '0'),
    ('moderation_queue', '0')
ON CONFLICT (collection_name) DO NOTHING;