CREATE OR REPLACE VIEW analytics.daily_user_activity AS
WITH page_counts AS (
    SELECT 
        DATE(s.start_time) AS activity_date,
        s.user_id,
        s.session_id,
        s.pages_visited,
        s.actions,
        s.device_type,
        s.session_duration_minutes,
        unnest(s.pages_visited) AS page
    FROM raw_data.user_sessions s
    WHERE s.start_time IS NOT NULL
)
SELECT 
    activity_date,
    user_id,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(session_duration_minutes) AS total_time_minutes,
    AVG(session_duration_minutes) AS avg_session_duration,
    COUNT(DISTINCT page) AS unique_pages_visited,
    COUNT(*) FILTER (WHERE 'purchase' = ANY(actions)) AS purchase_sessions,
    COUNT(*) FILTER (WHERE 'add_to_cart' = ANY(actions)) AS add_to_cart_sessions,
    COUNT(*) FILTER (WHERE device_type = 'mobile') AS mobile_sessions,
    COUNT(*) FILTER (WHERE device_type = 'desktop') AS desktop_sessions,
    COUNT(*) FILTER (WHERE device_type = 'tablet') AS tablet_sessions
FROM page_counts
GROUP BY activity_date, user_id
ORDER BY activity_date DESC, user_id;

CREATE OR REPLACE VIEW analytics.user_engagement_summary AS
SELECT 
    s.user_id,
    COUNT(DISTINCT s.session_id) AS lifetime_sessions,
    SUM(s.session_duration_minutes) AS lifetime_minutes,
    AVG(s.session_duration_minutes) AS avg_session_length,
    COUNT(DISTINCT r.recommended_products) AS products_recommended,
    COUNT(DISTINCT m.review_id) AS total_reviews,
    AVG(m.rating) AS avg_rating,
    MAX(s.start_time) AS last_activity,
    MIN(s.start_time) AS first_activity,
    EXTRACT(DAY FROM NOW() - MAX(s.start_time)) AS days_since_last_activity
FROM raw_data.user_sessions s
LEFT JOIN raw_data.user_recommendations r ON s.user_id = r.user_id
LEFT JOIN raw_data.moderation_queue m ON s.user_id = m.user_id
GROUP BY s.user_id;

CREATE OR REPLACE VIEW analytics.support_ticket_stats AS
SELECT 
    DATE(created_at) AS ticket_date,
    status,
    issue_type,
    COUNT(*) AS ticket_count,
    COUNT(DISTINCT user_id) AS unique_users,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) AS avg_resolution_hours,
    MIN(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) AS min_resolution_hours,
    MAX(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) AS max_resolution_hours,
    COUNT(*) FILTER (WHERE status = 'open') AS open_tickets,
    COUNT(*) FILTER (WHERE status = 'closed') AS closed_tickets,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_tickets
FROM raw_data.support_tickets
GROUP BY DATE(created_at), status, issue_type
ORDER BY ticket_date DESC, issue_type;

CREATE OR REPLACE VIEW analytics.support_queue_current AS
SELECT 
    issue_type,
    COUNT(*) AS total_open,
    MIN(EXTRACT(EPOCH FROM (NOW() - created_at))/3600) AS oldest_ticket_hours,
    MAX(EXTRACT(EPOCH FROM (NOW() - created_at))/3600) AS newest_ticket_hours,
    AVG(EXTRACT(EPOCH FROM (NOW() - created_at))/3600) AS avg_waiting_hours,
    COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (NOW() - created_at))/3600 > 24) AS tickets_older_than_24h,
    COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (NOW() - created_at))/3600 > 48) AS tickets_older_than_48h
FROM raw_data.support_tickets
WHERE status IN ('open', 'pending')
GROUP BY issue_type
ORDER BY total_open DESC;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.support_dashboard AS
SELECT 
    NOW() AS refreshed_at,
    (SELECT COUNT(*) FROM raw_data.support_tickets WHERE status = 'open') AS open_tickets_count,
    (SELECT COUNT(*) FROM raw_data.support_tickets WHERE status = 'pending') AS pending_tickets_count,
    (SELECT COUNT(*) FROM raw_data.support_tickets WHERE created_at >= NOW() - INTERVAL '24 hours') AS tickets_last_24h,
    (SELECT ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600)::NUMERIC, 2) 
     FROM raw_data.support_tickets WHERE status = 'closed') AS avg_resolution_time_hours,
    (SELECT issue_type FROM raw_data.support_tickets 
     GROUP BY issue_type ORDER BY COUNT(*) DESC LIMIT 1) AS most_common_issue,
    (SELECT COUNT(DISTINCT user_id) FROM raw_data.support_tickets 
     WHERE created_at >= NOW() - INTERVAL '7 days') AS unique_users_last_7d;

CREATE UNIQUE INDEX IF NOT EXISTS idx_support_dashboard_refreshed ON analytics.support_dashboard(refreshed_at);