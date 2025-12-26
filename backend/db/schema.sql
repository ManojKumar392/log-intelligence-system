CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    service TEXT,
    level TEXT,
    severity INT,
    message TEXT,
    metadata JSONB,
    processed_at TIMESTAMPTZ
);
