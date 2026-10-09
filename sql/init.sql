CREATE TABLE customers (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE accounts (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    balance     NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE transactions (
    id         SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES accounts(id),
    type       TEXT NOT NULL CHECK (type IN ('deposit', 'withdrawal')),
    amount     NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ DEFAULT now()
);
