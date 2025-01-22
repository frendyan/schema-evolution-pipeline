

SELECT 'CREATE DATABASE dwh'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh')\gexec

\c dwh;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.user_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
