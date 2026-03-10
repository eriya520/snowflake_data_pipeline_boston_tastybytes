-- snowflake:ignore-jinja
use role accountadmin;
use database staging_tasty_bytes;
use schema raw_pos;

--configure logging level
ALTER ACCOUNT SET LOG_LEVEL = "INFO";

--create the stored procedure, define its logic with snowpark for python, write sales to raw_pos.daily_sales_boston_t
CREATE OR REPLACE PROCEDURE staging_tasty_bytes.raw_pos.process_order_headers_stream()
    returns string
    language Python
    runtime_version = "3.10"
    handler='process_order_headers_stream'
    packages = ('snowflake-snowpark-python')
as
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import logging

def process_order_headers_stream(session:Session) -> float:
    logger = logging.getLogger("order_header_stream_sproc")
    # log procedure start:
    logger.info("Starting process_order_headers_stream procedure")
    try:
        # query the stream
        logger.info("Querying order_header stream for new records")
        recent_orders = session.table("order_header_stream").filter(F.col("METADATA$ACTION")=="INSERT")

        # look up location of orders in stream using LOCATIONS table
        logger.info("Filtering orders for Boston, USA")
        locations = session.table("location")
        boston_orders = recent_orders.join(
                                locations, recent_orders["LOCATION_ID"]==locations["LOCATION_ID"]
                                ).filter(
                                    (locations["CITY"]=="Boston") & 
                                    (locations["COUNTRY"]=="United States")
                                    )

        # log the count of filtered records:
        boston_count = boston_orders.count()
        logger.info(f"Total number of new records for Boston, USA: {boston_count}")

        # log successful completion
        logger.info("Procedure complete successfully")
    except Exception as e:
        # log any errors that occur
        logger.error(f"Error processing orders: {str(e)}")
        raise
    $$;

        CALL staging_tasty_bytes.raw_pos.process_order_headers_stream();

    -- set up the context for event table
        use database staging_tasty_bytes;
        use schema telemetry;
        select * from pipeline_events;


        -- Insert dummy data into ORDER_HEADER table
INSERT INTO STAGING_TASTY_BYTES.RAW_POS.ORDER_HEADER (
    ORDER_ID,
    TRUCK_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    DISCOUNT_ID,
    SHIFT_ID,
    SHIFT_START_TIME,
    SHIFT_END_TIME,
    ORDER_CHANNEL,
    ORDER_TS,
    SERVED_TS,
    ORDER_CURRENCY,
    ORDER_AMOUNT,
    ORDER_TAX_AMOUNT,
    ORDER_DISCOUNT_AMOUNT,
    ORDER_TOTAL
) VALUES 
-- Order 1
(1001, 42, 137, 5001, NULL, 301, '08:00:00', '16:00:00', 'POS', 
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'USD', 25.50, '3.83', '0.00', 29.33),

--  Order 2
(1002, 42, 137, 5002, 'SUMMER10', 301, '08:00:00', '16:00:00', 'MOBILE', 
 DATEADD(hour, -1, CURRENT_TIMESTAMP()), DATEADD(minute, -45, CURRENT_TIMESTAMP()), 'EUR', 42.75, '6.41', '4.28', 44.88),

-- Hamburg Order 3
(1003, 43, 137, 5003, NULL, 302, '10:00:00', '18:00:00', 'POS', 
 DATEADD(hour, -3, CURRENT_TIMESTAMP()), DATEADD(hour, -3, CURRENT_TIMESTAMP()), 'EUR', 18.20, '2.73', '0.00', 20.93);
 
CALL staging_tasty_bytes.raw_pos.process_order_headers_stream();

SELECT * FROM pipeline_events;

SELECT * FROM pipeline_events WHERE record_type = 'LOG';

--INSERT DUMMY DATA INTO ORDER_HEADER TABLE
insert into staging_tasty_bytes.raw_pos.order_header(
    ORDER_ID,
    TRUCK_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    DISCOUNT_ID,
    SHIFT_ID,
    SHIFT_START_TIME,
    SHIFT_END_TIME,
    ORDER_CHANNEL,
    ORDER_TS,
    SERVED_TS,
    ORDER_CURRENCY,
    ORDER_AMOUNT,
    ORDER_TAX_AMOUNT,
    ORDER_DISCOUNT_AMOUNT,
    ORDER_TOTAL
) VALUES
-- Order 1
(1001, 53, 15429, 5001, NULL, 301, '08:00:00', '16:00:00', 'POS', 
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'USD', 25.50, '3.83', '0.00', 29.33),
-- Order 2
(1002, 55, 5253, 5002, 'SUMMER10', 301, '08:00:00', '16:00:00', 'MOBILE', 
 DATEADD(hour, -1, CURRENT_TIMESTAMP()), DATEADD(minute, -45, CURRENT_TIMESTAMP()), 'USD', 42.75, '6.41', '4.28', 44.88),
-- Order 3
(1003, 57, 4127, 5003, NULL, 302, '10:00:00', '18:00:00', 'POS', 
 DATEADD(hour, -3, CURRENT_TIMESTAMP()), DATEADD(hour, -3, CURRENT_TIMESTAMP()), 'USD', 18.20, '2.73', '0.00', 20.93);
 
CALL staging_tasty_bytes.raw_pos.process_order_headers_stream();

select * from pipeline_events;

select * from pipeline_events where record_type = 'LOG'