# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import altair as alt



# Write directly to the app
st.title(f"Food Truck Daily Sale Trends and Weather for Boston, MA")
st.write(
  """Boston daily sales and weather metric by year
  """
)

# Get the current credentials
session = get_active_session()

## get the correct environment
env = "STAGING"
# env = "PROD"

# Use an interactive slider to get user input
year = st.slider(
  "Select year",
  min_value=2020,
  max_value=2024,
  value=2021,
  help="Filter data by year",
)

# compute data boundairies
min_date = f"{year}-01-01"
max_date = f"{year}-12-31"

#  Create an example dataframe
#  Note: this is just some dummy data, but you can easily connect to your Snowflake data
#  It is also possible to query data using raw SQL using session.sql() e.g. session.sql("select * from table")
order_table = session.table(f"{env}_TASTY_BYTES.ANALYTICS.DAILY_CITY_METRICS_TRUCK_COUNT_V").select(
    col("DATE"),
    (col("DAILY_SALES")/1000).alias("DAILY_SALES"),
    col("AVG_TEMPERATURE_FAHRENHEIT"),
    col("AVG_PRECIPITATION_INCHES"),
    col("MAX_WIND_SPEED_100M_MPH"),
    col("NUMBER_OF_TRUCKS")
    ).filter((col("DATE")>= min_date) & (col('DATE')<= max_date)).to_pandas()


zero_sales = order_table.loc[order_table['DAILY_SALES']==0]

# Map column names to desired legend titles
weather_labels = {
    'AVG_TEMPERATURE_FAHRENHEIT': 'Avg Temperature (°F)',
    'AVG_PRECIPITATION_INCHES': 'Avg precipitation (in)',
    'MAX_WIND_SPEED_100M_MPH': 'Max Wind Speed (mph)',
 
}
if order_table.shape[0]>0:
    Boston_weather_long = order_table.melt(id_vars='DATE',value_vars=weather_labels, var_name='Measure', value_name='Value')
    Boston_weather_long['Measure'] = Boston_weather_long['Measure'].replace(weather_labels)

if zero_sales.shape[0]>0:
    Boston_zero_sales = zero_sales.melt(id_vars='DATE', value_vars=weather_labels, var_name = 'Measure', value_name='Value')
    Boston_zero_sales['Measure'] = Boston_zero_sales['Measure'].replace(weather_labels)

if order_table.shape[0]>0:
    # --- Row 1: Sales (all dates) ---
    row1 = (
        alt.Chart(order_table)
        .mark_bar(color="#1f77b4")
        .encode(
            x=alt.X("DATE:T", title="Date"),
            y=alt.Y("DAILY_SALES:Q", title="Daily Sales ($1k)", scale=alt.Scale(zero=False)),
        )
        .properties(height=200, title=f"Daily Sales in {year}")
    )
    # --- ROW2: Number of trucks
    row2 = (
        alt.Chart(order_table)
        .mark_circle(size=40, color="#e45756")
        .encode(
            x="DATE:T",
            y=alt.Y("NUMBER_OF_TRUCKS:Q", title="Number of Trucks", scale=alt.Scale(zero=False)),
            tooltip=["DATE:T", "NUMBER_OF_TRUCKS:Q"]
        )
        .properties(height=200, title=f"Number of Trucks in {year}")
    )
    
    # --- Row 3: Weather metrics (all dates) ---
    row3 = (
        alt.Chart(Boston_weather_long)
        .mark_circle(size=40)
        .encode(
            x="DATE:T",
            y=alt.Y("Value:Q", title="Weather Metrics", scale=alt.Scale(zero=False)),
            color=alt.Color("Measure:N", title="Metric")
        )
        .properties(height=200, title=f"Weather Metrics in {year}")
    )
    
    if zero_sales.shape[0]>0:
        # --- Row 4: Weather metrics (zero-sales subset) ---
        row4 = (
            alt.Chart(Boston_zero_sales)
            .mark_circle(size=40)
            .encode(
                x="DATE:T",
                y=alt.Y("Value:Q", title="Weather Metrics (Zero Sales)", scale=alt.Scale(zero=False)),
                color=alt.Color("Measure:N", title="Metric")
            )
            .properties(height=200, title=f"Weather Metrics on Zero-Sales Days in {year}")
        )
        
        
        # --- Combine vertically ---
        final_chart = (
            alt.vconcat(row1, row2, row3, row4)
            .resolve_scale(x="shared")
            .properties(autosize='fit')
        )
    else:
         # --- Combine vertically ---
        final_chart = (
            alt.vconcat(row1, row2, row3)
            .resolve_scale(x="shared")
            .properties(autosize='fit')
        )
    final_chart
else:
    st.write(f"No sales table available in {year} for Boston!")