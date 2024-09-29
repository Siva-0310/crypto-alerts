-- Connect to the crypto database
\c crypto

-- Create the users table
CREATE TABLE users (
    id VARCHAR(128) PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    email VARCHAR(128) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW(),
    is_verified BOOLEAN DEFAULT FALSE
);

-- Create the alerts table
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(8) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    is_triggered BOOLEAN DEFAULT FALSE,
    is_above BOOLEAN DEFAULT FALSE,
    price NUMERIC(10, 2) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT un_alert UNIQUE(coin, user_id, price)
);