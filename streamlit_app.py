# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import altair as alt
import calendar


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
year = st.selectbox("Select year",
                   options=[2020,2021, 2022],
                   index=1,
                   help='Filter data by year')
month_name = st.selectbox('Select Month', 
                  options=list(calendar.month_name)[1:],
                  help='Filter data by month')

duration = st.selectbox("Select duration",
                       options=['year','month','week'],
                       help='Choose aggregation level')

if duration == 'year':
# compute data boundairies
    min_date = f"{year}-01-01"
    max_date = f"{year}-12-31"
    title_duration = f'{year}'
elif duration == 'month':
    month = list(calendar.month_name).index(month_name)
    last_day = calendar.monthrange(year, month)[1]
    min_date = f'{year}-{month:02d}-01'
    max_date = f'{year}-{month:02d}-{last_day:02d}'
    title_duration = f'{year}-{month:02d}'

elif duration == "week":
    import datetime

    # pick first day of selected year
    start_of_year = datetime.date(year, 1, 1)
    month = list(calendar.month_name).index(month_name)
    # compute week number dropdown if needed
    week = st.number_input("Select week number", min_value=1, max_value=52, value=1)

    min_date = start_of_year + datetime.timedelta(weeks=week-1)
    max_date = min_date + datetime.timedelta(days=6)

    min_date = min_date.strftime("%Y-%m-%d")
    max_date = max_date.strftime("%Y-%m-%d")
    title_duration = f'{year}-{month:02d} week-{week}'


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


zero_sales = order_table.loc[order_table['DAILY_SALES']==0.0]

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
        .properties(height=200, title=f"Daily Sales in "+title_duration)
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
        .properties(height=200, title=f"Number of Trucks in "+title_duration)
    )
    
    # --- Row 3: Weather metrics (all dates) ---
    primary_metric_chart= (
        alt.Chart(Boston_weather_long)
        .transform_filter(alt.FieldOneOfPredicate(field='Measure', 
                                                  oneOf=["Avg Temperature (°F)", "Max Wind Speed (mph)"]))
        .mark_circle(size=40)
        .encode(
            x="DATE:T",
            y=alt.Y("Value:Q", title="Avg Temperature(°F)/Max WindSpeed (mph)", scale=alt.Scale(zero=False)),
            color=alt.Color("Measure:N", title="Metric")
        )
        .properties(height=200, title=f"Weather Metrics in "+title_duration)
    )
    secondary_metric_chart =(
    alt.Chart(Boston_weather_long)
    .transform_filter(alt.datum.Measure == "Avg precipitation (in)")
    .mark_circle(size=40)
    .encode(
        x="DATE:T",
        y=alt.Y(
            "Value:Q",
            title="Average Precipitation (in)",
            scale=alt.Scale(zero=False)
            ), 
            color=alt.value('purple')
        )
    )
    # --laye rwith independent y-scales
    row3 = (
        alt.layer(primary_metric_chart, secondary_metric_chart)
        .resolve_scale(y='independent')
        .properties(height=200, title = f"Weather Metrics in "+title_duration)
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
            .properties(height=200, title=f"Weather Metrics on Zero-Sales Days in "+title_duration)
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