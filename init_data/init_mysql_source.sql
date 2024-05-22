-- Create user table:
CREATE TABLE IF NOT EXISTS users (
    user_id INT AUTO_INCREMENT PRIMARY KEY ,
    name VARCHAR(255),
    email VARCHAR(255),
    mobile VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
)
;

-- Ingest Data:


-- Create transaction table:
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id int AUTO_INCREMENT PRIMARY KEY ,
    user_id int ,
    order_id int,
    paid_price int,
    status int,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp
)
;
--

-- Create order table:
CREATE TABLE IF NOT EXISTS orders (
    order_id int,
    item_id int,
    num int ,
    price int,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp
)
;


-- Create menu table:
CREATE TABLE IF NOT EXISTS menus (
    item_id int AUTO_INCREMENT PRIMARY KEY ,
    name varchar(255),
    price int,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp
)
;

