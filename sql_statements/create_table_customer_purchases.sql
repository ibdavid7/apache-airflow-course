CREATE TABLE IF NOT EXISTS customer_purchases (
    id INT PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    product VARCHAR(100) NOT NULL,
    price INTEGER NOT NULL
);