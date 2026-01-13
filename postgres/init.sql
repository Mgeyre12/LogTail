-- This runs automatically when Postgres first starts

-- Create our test table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    role VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert some initial data
INSERT INTO users (name, email, role) VALUES
    ('Alice', 'alice@example.com', 'engineer'),
    ('Bob', 'bob@example.com', 'designer'),
    ('Charlie', 'charlie@example.com', 'manager');

-- Create the publication for CDC
CREATE PUBLICATION my_cdc_pub FOR TABLE users;

-- Show what we created
\dt
SELECT * FROM users;