--SILVER LAYER
--CUSTOMER_CLEAN

CREATE OR REPLACE TABLE SILVER.CUSTOMER_CLEAN (
    customer_id STRING,
    clean_customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    city STRING,
    state STRING,
    loyalty_tier STRING,
    created_at TIMESTAMP,
    source_file STRING
);


--PRODUCTS_CLEAN
CREATE OR REPLACE TABLE SILVER.PRODUCT_CLEAN (
    product_id STRING,
    clean_product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    price NUMBER(10,2),
    source_file STRING
);


--STORE_CLEAN
CREATE OR REPLACE TABLE SILVER.STORE_CLEAN (
    store_id STRING,
    clean_store_id STRING,
    store_name STRING,
    city STRING,
    state STRING,
    region STRING,
    source_file STRING
);

--SALES_HEADER_CLEAN
CREATE OR REPLACE TABLE SILVER.SALES_HEADER_CLEAN (
    transaction_id STRING,
    customer_id STRING,
    clean_customer_id STRING,
    store_id STRING,
    clean_store_id STRING,
    transaction_timestamp TIMESTAMP,
    total_amount NUMBER(12,2),
    source_file STRING
);

--SALES_LINE_ITEMS_CLEAN
CREATE OR REPLACE TABLE SILVER.SALES_LINE_ITEMS_CLEAN (
    line_item_id NUMBER,
    transaction_id STRING,
    product_id STRING,
    clean_product_id STRING,
    quantity NUMBER,
    price NUMBER(10,2),
    source_file STRING
);

--PROMOTION_CLEAN
CREATE OR REPLACE TABLE SILVER.PROMOTION_CLEAN AS
SELECT
    COALESCE(raw:promotion_id::STRING, 'UNKNOWN_' || SEQ8()) AS promotion_id,
    raw:promo_name::STRING AS promo_name,
    raw:discount_percent::FLOAT AS discount_percent,
    raw:start_date::DATE AS start_date,
    raw:end_date::DATE AS end_date,
    source_file
FROM BRONZE.PROMOTION_DETAILS_RAW;

--LOYALTY_RULES
CREATE OR REPLACE TABLE SILVER.LOYALTY_RULES_CLEAN AS
SELECT
    COALESCE(raw:rule_id::STRING, 'UNKNOWN_' || SEQ8()) AS rule_id,
    raw:tier::STRING AS tier,
    raw:points_per_100::FLOAT AS points_per_100,
    raw:benefits::STRING AS benefits,
    source_file
FROM BRONZE.LOYALTY_RULES_RAW;


--Transformations
--CUSTOMER TASK
INSERT INTO SILVER.CUSTOMER_CLEAN
SELECT
customer_id,
LPAD(REGEXP_REPLACE(customer_id,'[^0-9]',''),6,'0') AS clean_customer_id,
INITCAP(TRIM(first_name)),
INITCAP(TRIM(last_name)),
LOWER(TRIM(email)),
TRIM(phone),
INITCAP(TRIM(city)),
INITCAP(TRIM(state)),
UPPER(TRIM(loyalty_tier)),
created_at,
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY created_at DESC) rn
FROM BRONZE.CUSTOMER_STREAM
WHERE customer_id IS NOT NULL AND email IS NOT NULL
) WHERE rn = 1;

--PRODUCT TASK

INSERT INTO SILVER.PRODUCT_CLEAN
SELECT
product_id,
LPAD(REGEXP_REPLACE(product_id,'[^0-9]',''),6,'0'),
TRIM(product_name),
TRIM(category),
TRIM(brand),
TRY_TO_NUMBER(price::STRING),
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY product_id) rn
FROM BRONZE.PRODUCTS_STREAM
WHERE product_id IS NOT NULL AND price IS NOT NULL
) WHERE rn = 1;

-- STORE TASK

INSERT INTO SILVER.STORE_CLEAN
SELECT
store_id,
LPAD(REGEXP_REPLACE(store_id,'[^0-9]',''),4,'0'),
TRIM(store_name),
INITCAP(TRIM(city)),
INITCAP(TRIM(state)),
TRIM(region),
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY store_id) rn
FROM BRONZE.STORES_STREAM
WHERE store_id IS NOT NULL
) WHERE rn = 1;

--SALES HEADER TASK

INSERT INTO SILVER.SALES_HEADER_CLEAN
SELECT
transaction_id,
customer_id,
LPAD(REGEXP_REPLACE(customer_id,'[^0-9]',''),6,'0'),
store_id,
LPAD(REGEXP_REPLACE(store_id,'[^0-9]',''),4,'0'),
transaction_timestamp,
total_amount,
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_timestamp DESC) rn
FROM BRONZE.SALES_HEADER_STREAM
WHERE transaction_id IS NOT NULL
) WHERE rn = 1;

-- SALES LINE ITEMS TASK

INSERT INTO SILVER.SALES_LINE_ITEMS_CLEAN
SELECT
line_item_id,
transaction_id,
product_id,
LPAD(REGEXP_REPLACE(product_id,'[^0-9]',''),6,'0'),
quantity,
price,
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY line_item_id ORDER BY line_item_id) rn
FROM BRONZE.SALES_LINE_ITEMS_STREAM
WHERE line_item_id IS NOT NULL
) WHERE rn = 1;

-- PROMOTION TASKCREATE OR REPLACE TASK SILVER.TASK_LOYALTY_RULES_CLEAN

INSERT INTO SILVER.PROMOTION_CLEAN
SELECT
raw:promotion_id::STRING,
raw:promo_name::STRING,
TRY_TO_NUMBER(raw:discount_percent::STRING),
TRY_TO_DATE(raw:start_date::STRING),
TRY_TO_DATE(raw:end_date::STRING),
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY raw:promotion_id::STRING ORDER BY raw:start_date DESC) rn
FROM BRONZE.PROMOTIONS_STREAM
WHERE raw:promotion_id IS NOT NULL
) WHERE rn = 1;

️--LOYALTY TASK

INSERT INTO SILVER.LOYALTY_RULES_CLEAN
SELECT
raw:rule_id::STRING,
raw:tier::STRING,
TRY_TO_NUMBER(raw:points_per_100::STRING),
raw:benefits::STRING,
source_file
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY raw:rule_id::STRING ORDER BY raw:rule_id DESC) rn
FROM BRONZE.LOYALTY_RULES_STREAM
WHERE raw:rule_id IS NOT NULL
) WHERE rn = 1;
