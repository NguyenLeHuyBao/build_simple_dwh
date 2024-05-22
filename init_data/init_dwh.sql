-- Create Schema for raw data
CREATE SCHEMA raw_data;
CREATE SEQUENCE user_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE IF NOT EXISTS raw_data.users (
        user_key int default nextval('user_id_seq'::regclass) PRIMARY KEY ,
        user_id int,
        name varchar(255),
        email varchar(255),
        mobile varchar(255),
        location varchar(255),
        created_at timestamp DEFAULT now(),
        updated_at timestamp
)
;





CREATE SEQUENCE transaction_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE IF NOT EXISTS raw_data.transactions (
    transaction_key int default nextval('transaction_id_seq'::regclass) PRIMARY KEY ,
    user_id int ,
    order_id int,
    paid_price int,
    status int,
    location varchar(255),
    created_at timestamp DEFAULT now(),
    updated_at timestamp
)
;

CREATE SEQUENCE order_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE IF NOT EXISTS raw_data.orders (
    order_key int default nextval('order_id_seq'::regclass),
    order_id int,
    item_id int,
    num int ,
    price int,
    location varchar(255),
    created_at timestamp DEFAULT now(),
    updated_at timestamp
)
;

CREATE SEQUENCE menu_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE IF NOT EXISTS raw_data.menus (
    item_id int default nextval('menu_id_seq'::regclass) PRIMARY KEY ,
    name varchar(255),
    price int,
    location varchar(255),
    created_at timestamp DEFAULT now(),
    updated_at timestamp
)
;


-- Create Schema for processed data
CREATE SCHEMA processed_data;

-- Create Schema for fact table data
CREATE SCHEMA fact_tables;

