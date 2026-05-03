CREATE USER rep_user WITH REPLICATION PASSWORD 'secret';

CREATE TABLE users(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    details JSONB
);

ALTER TABLE users REPLICA IDENTITY FULL;

GRANT INSERT, UPDATE, DELETE ON users TO rep_user;

CREATE PUBLICATION my_publication FOR TABLE users;

SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');