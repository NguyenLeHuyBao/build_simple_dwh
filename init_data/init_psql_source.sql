-- Create user table:
CREATE SEQUENCE user_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE IF NOT EXISTS users (
    user_id int default nextval('user_id_seq'::regclass) PRIMARY KEY ,
    name varchar(255),
    email varchar(255),
    mobile varchar(255),
    created_at timestamp DEFAULT now(),
    updated_at timestamp
)
;


CREATE SEQUENCE cash_in_glxplay_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS cash_in_glxplay
(
    id                  INT         DEFAULT nextval('cash_in_glxplay_seq'::regclass) PRIMARY KEY,
    year_month          INT,
    date_key            INT,
    date_string         VARCHAR(10),
    charge_timestamp    TIMESTAMP,
    payment_type        VARCHAR(255),
    transid             VARCHAR(255),
    ammount             NUMERIC(15, 2),
    payment_type_method VARCHAR(255),
    card_type           VARCHAR(255),
    bank                VARCHAR(255),
    status              VARCHAR(255),
    source_file         VARCHAR(255),
    active_record       BOOL        DEFAULT TRUE,
    active_start        TIMESTAMP   DEFAULT now(),
    active_end          TIMESTAMP   DEFAULT '2222-01-01 00:00:00'::timestamp without time zone
)
;
-- Ingest Data:

-- Create transaction table:
--CREATE SEQUENCE transaction_id_seq START WITH 1 INCREMENT BY 1;
--CREATE TABLE IF NOT EXISTS transactions (
--    transaction_id int default nextval('transaction_id_seq'::regclass) PRIMARY KEY ,
--    user_id int ,
--    order_id int,
--    paid_price int,
--    status int,
--    created_at timestamp DEFAULT now(),
--    updated_at timestamp
--)
--;
--
---- Create order table:
--CREATE SEQUENCE order_id_seq START WITH 1 INCREMENT BY 1;
--CREATE TABLE IF NOT EXISTS orders (
--    order_id int,
--    item_id int,
--    num int ,
--    price int,
--    created_at timestamp DEFAULT now(),
--    updated_at timestamp
--)
--;
--
---- Create menu table:
--CREATE SEQUENCE menu_id_seq START WITH 1 INCREMENT BY 1;
--CREATE TABLE IF NOT EXISTS menus (
--    item_id int default nextval('menu_id_seq'::regclass) PRIMARY KEY ,
--    name varchar(255),
--    price int,
--    created_at timestamp DEFAULT now(),
--    updated_at timestamp
--)
--;
--
