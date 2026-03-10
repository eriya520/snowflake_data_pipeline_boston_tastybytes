-- snowflake:ignore-jinja
USE ROLE accountadmin;
USE DATABASE staging_tasty_bytes;
USE SCHEMA raw_pos;

-- Set account-level logging
ALTER ACCOUNT SET LOG_LEVEL = 'INFO';

-- Configure traces:
alter session set trace_level = always;

-- Create the stored procedure, define its logic with Snowpark for Python, write sales to raw_pos.daily_sales_Boston_t
CREATE OR REPLACE PROCEDURE staging_tasty_bytes.raw_pos.process_order_headers_stream()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER ='process_order_headers_stream'
  PACKAGES = ('snowflake-snowpark-python','snowflake-telemetry-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import logging
from snowflake import telemetry
import uuid

def process_order_headers_stream(session: Session) -> float:
# Set up basic logging
    logger = logging.getLogger('order_headers_stream_sproc')

    # Generate trace ID for this execution
    trace_id = str(uuid.uuid4())
    
    # Log procedure start
    logger.info("Starting process_order_headers_stream procedure")

    # Set initial span attributes for the entire procedure:
    telemetry.set_span_attribute("procedure", 'process_order_header_stream')
    telemetry.set_span_attribute("trace_id", trace_id)
    
    try:
        # Begin stream query span:
        telemetry.set_span_attribute("process_step",'query_stream')
        telemetry.add_event('query_begin',{"description":"Starting to query order_header_stream"})

        # Query the stream
        logger.info("Querying order_header_stream for new records")
        recent_orders = session.table("order_header_stream").filter(F.col("METADATA$ACTION") == "INSERT")

        # Record query completion event:
        telemetry.add_event('query_complete',{'description':'Completed query order_header_stream')
        # Begin location filtering span
        telemetry.set_span_attribute("process_step", "filter_locations")
        telemetry.add_event("filter_begin", {"description": "Filtering for Boston, United States"})
        
        # Look up location of the orders in the stream using the LOCATIONS table
        logger.info("Filtering orders for Boston, United States")
        locations = session.table("location")
        Boston_orders = recent_orders.join(
            locations,
            recent_orders["LOCATION_ID"] == locations["LOCATION_ID"]
        ).filter(
            (locations["CITY"] == "Boston") &
            (locations["COUNTRY"] == "United States")
        )
        
        # Log the count of filtered records
        Boston_count = Boston_orders.count()
        logger.info(f"Found {Boston_count} orders from Boston")
        
        '''
        # Calculate the sum of sales in Boston
        logger.info("Calculating daily sales aggregates")
        total_sales = Boston_orders.group_by(F.date_trunc('DAY', F.col("ORDER_TS"))).agg(
            F.coalesce(F.sum("ORDER_TOTAL"), F.lit(0)).alias("total_sales")
        )
        
        # Select the columns with proper aliases and convert to date type
        daily_sales = total_sales.select(
            F.date_trunc('DAY', F.col("ORDER_TS")).cast("DATE").alias("DATE"),
            F.col("total_sales")
        )
        
        # Write the results to the DAILY_SALES_Boston_T table
        logger.info("Writing results to raw_pos.daily_sales_boston_t")
        daily_sales.write.mode("append").save_as_table("raw_pos.daily_sales_boston_t")
        '''
        # Log successful completion
        logger.info("Procedure completed successfully")
        return "Daily sales for Boston, United States have been successfully written to raw_pos.daily_sales_Boston_t"
    
    except Exception as e:
        # Log any errors that occur
        logger.error(f"Error processing orders: {str(e)}")
        raise
$$;

-- Insert dummy data into ORDER_HEADER table
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

USE DATABASE staging_tasty_bytes;
USE SCHEMA TELEMETRY;

SELECT * FROM pipeline_events;

SELECT * FROM pipeline_events WHERE record_type = 'LOG';
SELECT * FROM pipeline_events WHERE record_type ILIKE '%SPAN%';

