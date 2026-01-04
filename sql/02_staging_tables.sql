-- Optional staging tables (if you load into SQL staging from ADF)
CREATE TABLE staging.transactions (
  transaction_id  BIGINT,
  customer_id     BIGINT,
  product_id      BIGINT,
  quantity        INT,
  amount          DECIMAL(18,2),
  transaction_ts  DATETIME2
);

CREATE TABLE staging.customers (
  customer_id BIGINT,
  full_name   VARCHAR(200),
  email       VARCHAR(200),
  country     VARCHAR(100),
  updated_ts  DATETIME2
);

CREATE TABLE staging.products (
  product_id   BIGINT,
  product_name VARCHAR(200),
  category     VARCHAR(100),
  price        DECIMAL(18,2),
  updated_ts   DATETIME2
);
