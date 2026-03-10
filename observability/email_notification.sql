-- snowflake:ignore-jinja

USE ROLE accountadmin;
USE DATABASE staging_tasty_bytes;
USE SCHEMA public;


-- Create an email integration 
CREATE OR REPLACE notification integration email_notification_int
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('ADD YOUR EMAIL HERE');  -- Update the recipient's email here

CREATE OR REPLACE PROCEDURE staging_tasty_bytes.raw_pos.notify_data_quality_team()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'notify_data_quality_team'
AS 
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from datetime import datetime

def notify_data_quality_team(session: Session) -> str:
    # Query the records with NULL values
    records = session.table("STAGING_TASTY_BYTES.RAW_POS.ORDER_HEADER") \
                            .filter((F.col("ORDER_AMOUNT").is_null()) | (F.col("ORDER_TOTAL").is_null())) \
                            .filter("ORDER_TS > DATEADD(hour, -6, CURRENT_TIMESTAMP())") \
                            .select(
                                F.col("ORDER_ID"),
                                F.col("TRUCK_ID"),
                                F.col("LOCATION_ID"),
                                F.col("ORDER_TS"),
                                F.col("ORDER_AMOUNT"),
                                F.col("ORDER_TOTAL")
                            )
    
    # Get a count of the problematic records
    record_count = records.count()
    
    if record_count == 0:
        return "No data quality issues found"

    # Convert the DataFrame to pandas for HTML formatting
    records_pd = records.to_pandas()
    
    # Convert the DataFrame to an HTML table with styling
    html_table = records_pd.to_html(index=False, classes='styled-table', na_rep='NULL')

    # Define the email content
    email_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
            }}
            h2 {{
                color: #FF4500;
            }}
            .alert-box {{
                background-color: #FFF0F0;
                border-left: 5px solid #FF4500;
                padding: 10px 15px;
                margin-bottom: 20px;
            }}
            .styled-table {{
                border-collapse: collapse;
                margin: 25px 0;
                font-size: 0.9em;
                font-family: 'Trebuchet MS', 'Lucida Sans Unicode', 'Lucida Grande', 'Lucida Sans', Arial, sans-serif;
                min-width: 400px;
                border-radius: 5px 5px 0 0;
                overflow: hidden;
                box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
            }}
            .styled-table thead tr {{
                background-color: #FF4500;
                color: #ffffff;
                text-align: left;
                font-weight: bold;
            }}
            .styled-table th,
            .styled-table td {{
                padding: 12px 15px;
            }}
            .styled-table tbody tr {{
                border-bottom: 1px solid #dddddd;
            }}
            .styled-table tbody tr:nth-of-type(even) {{
                background-color: #f3f3f3;
            }}
            .null-value {{
                color: #FF4500;
                font-weight: bold;
            }}
            .styled-table tbody tr:last-of-type {{
                border-bottom: 2px solid #FF4500;
            }}
        </style>
    </head>
    <body>
        <h2>⚠️ NULL values detected: ORDER_AMOUNT, ORDER_TOTAL </h2>
        <div class="alert-box">
            <p><strong>Alert Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Issue:</strong> {record_count} order(s) found with missing ORDER_AMOUNT or ORDER_TOTAL values in the past 6 hours</p>
        </div>
        <p>The following orders have NULL values that require attention:</p>
        {html_table}
        <p><i>Please investigate these records and update the missing values as soon as possible. NULL financial values can impact revenue reporting and analytics.</i></p>
    </body>
    </html>
    """
    
    # Send the email:
    session.call("SYSTEM$SEND_EMAIL",
                 "email_notification_int",
                 "ADD YOUR EMAIL HERE",
                 f"ALERT: {record_count} orders with NULL values detected",
                 email_content,
                 "text/html")
    
    # Return a success message with the count of problematic records
    return f"Data quality alert sent successfully. {record_count} records reported."
$$;


CREATE TABLE staging_tasty_bytes.telemetry.data_quality_alerts (
  alert_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  alert_name VARCHAR,
  severity VARCHAR,
  message VARCHAR,
  record_count INTEGER
);

-- Create a serverless alert with a schedule:
CREATE OR REPLACE ALERT order_data_quality_alert
  SCHEDULE = '30 MINUTES'
  IF (EXISTS (
    SELECT * FROM STAGING_TASTY_BYTES.RAW_POS.ORDER_HEADER 
    WHERE (ORDER_AMOUNT IS NULL OR ORDER_TOTAL IS NULL) 
    AND ORDER_TS > DATEADD(hour, -6, CURRENT_TIMESTAMP())
  ))
  THEN 
    BEGIN
      -- Insert a record into the table
      INSERT INTO staging_tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, message, record_count)
      SELECT 
        'ORDER_HEADER_NULL_VALUES', 
        'ERROR', 
        'Data quality issue detected: missing amount or total values', 
        COUNT(*)
      FROM STAGING_TASTY_BYTES.RAW_POS.ORDER_HEADER 
      WHERE (ORDER_AMOUNT IS NULL OR ORDER_TOTAL IS NULL) 
      AND ORDER_TS > DATEADD(hour, -6, CURRENT_TIMESTAMP());
        
      -- Call stored procedure for notification:
      call staging_tasty_bytes.raw_pos.notify_data_quality_team();
      
    END;

-- Check alert status
SHOW ALERTS LIKE 'order_data_quality_alert';

-- Start the alert
ALTER ALERT order_data_quality_alert RESUME;

-- Execute the alert IMMEDIATELY for test:
execute alert order_data_quality_alert;

-- Insert dummy data with missing ORDER_AMOUNT or ORDER_TOTAL
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
    ORDER_AMOUNT,  -- Missing value (NULL)
    ORDER_TAX_AMOUNT,
    ORDER_DISCOUNT_AMOUNT,
    ORDER_TOTAL
) VALUES 
-- Record with missing ORDER_AMOUNT
(2001, 55, 142, 6001, NULL, 401, '09:00:00', '17:00:00', 'POS', 
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'USD', NULL, '2.50', '0.00', 30.45),

-- Record with missing ORDER_TOTAL
(2002, 55, 142, 6002, 'PROMO25', 401, '09:00:00', '17:00:00', 'MOBILE', 
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'USD', 24.95, '2.25', '6.24', NULL),

-- Record with both ORDER_AMOUNT and ORDER_TOTAL missing
(2003, 56, 143, 6003, NULL, 402, '10:00:00', '18:00:00', 'WEB', 
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'USD', NULL, '1.88', '0.00', NULL);

-- Check for alerts in data_quality_alerts table
USE DATABASE staging_tasty_bytes;
USE SCHEMA TELEMETRY;
SELECT * FROM data_quality_alerts;

-- Suspend the alert
ALTER ALERT order_data_quality_alert SUSPEND;
