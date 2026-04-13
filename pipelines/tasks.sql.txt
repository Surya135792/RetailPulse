--TASKS : Scheduled sql jobs like (cron jobs)
--INCREMENTAL CLEANING . IT READS FROM STREAMS

--CUSTOMER TASK
CREATE OR REPLACE TASK SILVER.TASK_CUSTOMER_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.CUSTOMER_STREAM')
AS
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
CREATE OR REPLACE TASK SILVER.TASK_PRODUCT_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.PRODUCTS_STREAM')
AS
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
CREATE OR REPLACE TASK SILVER.TASK_STORE_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STORES_STREAM')
AS
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
CREATE OR REPLACE TASK SILVER.TASK_SALES_HEADER_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.SALES_HEADER_STREAM')
AS
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
CREATE OR REPLACE TASK SILVER.TASK_SALES_LINE_ITEMS_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.SALES_LINE_ITEMS_STREAM')
AS
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

-- PROMOTION TASK
CREATE OR REPLACE TASK SILVER.TASK_PROMOTION_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.PROMOTIONS_STREAM')
AS
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
CREATE OR REPLACE TASK SILVER.TASK_LOYALTY_RULES_CLEAN
WAREHOUSE = RETAIL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.LOYALTY_RULES_STREAM')
AS
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


--GOLD DAG PIPELINE
--ROOT TASK

--STEP 1: ROOT TASK (UNCHANGED)
CREATE OR REPLACE TASK GOLD.START_GOLD_PIPELINE
WAREHOUSE = RETAIL_WH
SCHEDULE='1 MINUTE'
AS
SELECT 1;
-- STEP 2: DIM TASKS (FIXED)
--DIM CUSTOMER (SCD TYPE 2 — FIXED)
CREATE OR REPLACE TASK GOLD.TASK_DIM_CUSTOMER
WAREHOUSE=RETAIL_WH
AFTER GOLD.START_GOLD_PIPELINE
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.CUSTOMER_CLEAN_STREAM')
AS
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
CREATE OR REPLACE TASK GOLD.TASK_DIM_PRODUCT
WAREHOUSE=RETAIL_WH
AFTER GOLD.START_GOLD_PIPELINE
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.PRODUCT_CLEAN_STREAM')
AS
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
CREATE OR REPLACE TASK GOLD.TASK_DIM_STORE
WAREHOUSE=RETAIL_WH
AFTER GOLD.START_GOLD_PIPELINE
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.STORE_CLEAN_STREAM')
AS
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
CREATE OR REPLACE TASK GOLD.TASK_DIM_PROMOTION
WAREHOUSE=RETAIL_WH
AFTER GOLD.START_GOLD_PIPELINE
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.PROMOTION_CLEAN_STREAM')
AS
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
CREATE OR REPLACE TASK GOLD.TASK_DIM_LOYALTY_RULES
WAREHOUSE=RETAIL_WH
AFTER GOLD.START_GOLD_PIPELINE
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.LOYALTY_RULES_CLEAN_STREAM')
AS
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

CREATE OR REPLACE TASK GOLD.TASK_FACT_SALES_HEADER
WAREHOUSE=RETAIL_WH
AFTER GOLD.TASK_DIM_CUSTOMER, GOLD.TASK_DIM_STORE   -- ✅ FIXED DAG
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.SALES_HEADER_CLEAN_STREAM')
AS

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
CREATE OR REPLACE TASK GOLD.TASK_FACT_SALES_LINE_ITEMS
WAREHOUSE=RETAIL_WH
AFTER GOLD.TASK_DIM_PRODUCT   -- ✅ FIXED DAG
WHEN SYSTEM$STREAM_HAS_DATA('SILVER.SALES_LINE_ITEMS_CLEAN_STREAM')
AS

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

