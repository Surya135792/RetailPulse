--GOLD LAYER

--DIMENSIONAL TABLES
--DIM_CUSTOMER (SCD TYPE-2)
--Here we used SCD TYPE -2 logic, so it doesnt required adding source_file, and dim_tables are not file_based transactions
CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMER (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id STRING,
    clean_customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    city STRING,
    state STRING,
    loyalty_tier STRING,
    start_date DATE,
    end_date DATE,
    is_current STRING
);

--DIM_PRODUCT
CREATE OR REPLACE TABLE GOLD.DIM_PRODUCT (
    product_sk NUMBER AUTOINCREMENT,
    product_id STRING,
    clean_product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    price NUMBER(10,2)
);

--DIM_STORE
CREATE OR REPLACE TABLE GOLD.DIM_STORE (
    store_sk NUMBER AUTOINCREMENT,
    store_id STRING,
    clean_store_id STRING,
    store_name STRING,
    city STRING,
    state STRING,
    region STRING
);

--DIM_PROMOTIONS
CREATE OR REPLACE TABLE GOLD.DIM_PROMOTION
(
promotion_sk NUMBER AUTOINCREMENT,
promotion_id STRING,
promo_name STRING,
discount_percent NUMBER(5,2),
start_date DATE,
end_date DATE
);

--DIM_LOYALITY_RULES
CREATE OR REPLACE TABLE GOLD.DIM_LOYALTY_RULES
(
rule_sk NUMBER AUTOINCREMENT,
rule_id STRING,
tier STRING,
points_per_100 NUMBER(5,2),
benefits STRING
);

--fact tables needs source_file in it , because it stores transactions coming directly from files
--FACT_SALES_HEADER
CREATE OR REPLACE TABLE GOLD.FACT_SALES_HEADER
(
transaction_id STRING,
customer_sk NUMBER,
store_sk NUMBER,
transaction_timestamp TIMESTAMP,
total_amount NUMBER(12,2)
);

--FACT_SALES_LINE_ITEMS
CREATE OR REPLACE TABLE GOLD.FACT_SALES_LINE_ITEMS
(
line_item_id NUMBER,
transaction_id STRING,
product_sk NUMBER,
quantity NUMBER,
price NUMBER(10,2)
);

ALTER TABLE GOLD.FACT_SALES_HEADER
ADD COLUMN source_file STRING;

ALTER TABLE GOLD.FACT_SALES_LINE_ITEMS
ADD COLUMN source_file STRING;



--Gold fact and Dimensions
--DIM CUSTOMER (SCD TYPE 2 — FIXED)

MERGE INTO GOLD.DIM_CUSTOMER T
USING SILVER.CUSTOMER_CLEAN_STREAM S
ON T.clean_customer_id = S.clean_customer_id
AND T.is_current='Y'

WHEN MATCHED AND (
NVL(T.city,'') <> NVL(S.city,'') OR
NVL(T.email,'') <> NVL(S.email,'') OR
NVL(T.loyalty_tier,'') <> NVL(S.loyalty_tier,'')
)

THEN UPDATE SET
end_date = CURRENT_DATE,
is_current='N'

WHEN NOT MATCHED THEN
INSERT (
customer_id, clean_customer_id, first_name, last_name,
email, phone, city, state, loyalty_tier,
start_date, end_date, is_current
)
VALUES (
S.customer_id, S.clean_customer_id, S.first_name, S.last_name,
S.email, S.phone, S.city, S.state, S.loyalty_tier,
CURRENT_DATE, NULL, 'Y'
);

-- DIM PRODUCT

MERGE INTO GOLD.DIM_PRODUCT T
USING SILVER.PRODUCT_CLEAN_STREAM S
ON T.clean_product_id=S.clean_product_id

WHEN MATCHED THEN UPDATE SET
product_name=S.product_name,
category=S.category,
brand=S.brand,
price=S.price
 
WHEN NOT MATCHED THEN
INSERT (product_id,clean_product_id,product_name,category,brand,price)
VALUES (S.product_id,S.clean_product_id,S.product_name,S.category,S.brand,S.price);

-- DIM STORE

MERGE INTO GOLD.DIM_STORE T
USING SILVER.STORE_CLEAN_STREAM S
ON T.clean_store_id=S.clean_store_id

WHEN MATCHED THEN UPDATE SET
store_name=S.store_name,
city=S.city,
state=S.state,
region=S.region

WHEN NOT MATCHED THEN
INSERT (store_id,clean_store_id,store_name,city,state,region)
VALUES (S.store_id,S.clean_store_id,S.store_name,S.city,S.state,S.region);

--DIM PROMOTION

MERGE INTO GOLD.DIM_PROMOTION T
USING SILVER.PROMOTION_CLEAN_STREAM S
ON T.promotion_id=S.promotion_id

WHEN MATCHED THEN UPDATE SET
promo_name=S.promo_name,
discount_percent=S.discount_percent,
start_date=S.start_date,
end_date=S.end_date

WHEN NOT MATCHED THEN
INSERT (promotion_id,promo_name,discount_percent,start_date,end_date)
VALUES (S.promotion_id,S.promo_name,S.discount_percent,S.start_date,S.end_date);

--DIM LOYALTY

MERGE INTO GOLD.DIM_LOYALTY_RULES T
USING SILVER.LOYALTY_RULES_CLEAN_STREAM S
ON T.rule_id=S.rule_id

WHEN MATCHED THEN UPDATE SET
tier=S.tier,
points_per_100=S.points_per_100,
benefits=S.benefits

WHEN NOT MATCHED THEN
INSERT (rule_id,tier,points_per_100,benefits)
VALUES (S.rule_id,S.tier,S.points_per_100,S.benefits);

--STEP 3: FACT TASKS (FIXED — MOST IMPORTANT)
--FACT SALES HEADER (FIXED JOIN ISSUE)

MERGE INTO GOLD.FACT_SALES_HEADER T
USING ( SELECT * FROM (SELECT
S.transaction_id,
C.customer_sk,
ST.store_sk,
S.transaction_timestamp,
S.total_amount,
S.source_file,
ROW_NUMBER() OVER ( PARTITION BY S.transaction_id ORDER BY S.transaction_timestamp DESC) rn
FROM SILVER.SALES_HEADER_CLEAN_STREAM S

-- ✅ safe joins
JOIN GOLD.DIM_CUSTOMER C
ON S.clean_customer_id = C.clean_customer_id
AND C.is_current='Y'
JOIN GOLD.DIM_STORE ST
ON S.clean_store_id = ST.clean_store_id
)
WHERE rn = 1  --keeps latest record
) SRC
ON T.transaction_id = SRC.transaction_id
WHEN MATCHED THEN
UPDATE SET
customer_sk = SRC.customer_sk,
store_sk = SRC.store_sk,
transaction_timestamp = SRC.transaction_timestamp,
total_amount = SRC.total_amount
WHEN NOT MATCHED THEN
INSERT (
transaction_id, customer_sk, store_sk,
transaction_timestamp, total_amount, source_file
)
VALUES (
SRC.transaction_id, SRC.customer_sk, SRC.store_sk,
SRC.transaction_timestamp, SRC.total_amount, SRC.source_file
);





--FACT SALES LINE ITEMS (FIXED)

MERGE INTO GOLD.FACT_SALES_LINE_ITEMS T
USING (
SELECT * FROM ( SELECT
S.line_item_id,
S.transaction_id,
P.product_sk,
S.quantity,
S.price,
S.source_file,
ROW_NUMBER() OVER (PARTITION BY S.line_item_id ORDER BY S.line_item_id) rn

FROM SILVER.SALES_LINE_ITEMS_CLEAN_STREAM S 
-- ✅ safe join
JOIN GOLD.DIM_PRODUCT P
ON S.clean_product_id = P.clean_product_id
)
WHERE rn = 1
) SRC

ON T.line_item_id = SRC.line_item_id
WHEN MATCHED THEN
UPDATE SET
transaction_id = SRC.transaction_id,
product_sk = SRC.product_sk,
quantity = SRC.quantity,
price = SRC.price

WHEN NOT MATCHED THEN
INSERT (
line_item_id, transaction_id, product_sk,
quantity, price, source_file
)
VALUES (
SRC.line_item_id, SRC.transaction_id, SRC.product_sk,
SRC.quantity, SRC.price, SRC.source_file
);

