import json

import holidays
import pandas as pd

print("Starting script to generate date dimension CSV...")

# 1. Configuration
START_DATE = '2024-01-01'
END_DATE = '2040-12-31'
OUTPUT_FILE = 'dim_date.jsonl'

# 2. Set up market holiday calendars
# The holidays library handles complex rules, like holidays on weekends.
us_holidays = holidays.US(years=range(int(START_DATE[:4]), int(END_DATE[:4]) + 2))
ar_holidays = holidays.AR(years=range(int(START_DATE[:4]), int(END_DATE[:4]) + 2))

market_holiday_lookups = {
    'US': us_holidays,
    'AR': ar_holidays
}

# 3. Create a DataFrame with the full date range
print(f"Generating dates from {START_DATE} to {END_DATE}...")
df = pd.DataFrame({'full_date': pd.to_datetime(pd.date_range(START_DATE, END_DATE))})

# 4. Populate date component columns
print("Calculating date components (year, month, day, etc.)...")
df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
df['year'] = df['full_date'].dt.year
df['quarter'] = df['full_date'].dt.quarter
df['month'] = df['full_date'].dt.month
df['month_name'] = df['full_date'].dt.strftime('%B')
df['day'] = df['full_date'].dt.day
# Adjust to match your schema: Monday=1, Sunday=7
df['day_of_week'] = df['full_date'].dt.dayofweek + 1 
df['day_of_week_name'] = df['full_date'].dt.strftime('%A')


# 5. Determine market status for each day
print("Determining market status and holidays for US and AR...")
def get_market_status(row_date):
    """Checks if a given date is a weekend or holiday for the defined markets."""
    open_markets = []
    closed_markets = []
    holiday_details = []
    
    # Check if the day is a weekend (Saturday or Sunday)
    is_weekend = row_date.dayofweek >= 5 
    
    for market, holiday_calendar in market_holiday_lookups.items():
        holiday_name = holiday_calendar.get(row_date)
        is_holiday = holiday_name is not None
        
        # A market is closed on weekends AND on its specific holidays
        if is_weekend or is_holiday:
            closed_markets.append(market)
            if is_holiday:
                # Add holiday details only if it's not a weekend holiday
                holiday_details.append({'market': market, 'name': holiday_name})
        else:
            open_markets.append(market)
            
    # Format for CSV: BigQuery can parse JSON strings during/after load
    return (
        json.dumps(open_markets),
        json.dumps(closed_markets),
        json.dumps(holiday_details)
    )

df[['markets_open', 'markets_closed', 'holidays']] = df['full_date'].apply(
    get_market_status
).apply(pd.Series)

# 6. Finalize DataFrame and save to CSV
print(f"Saving the data to {OUTPUT_FILE}...")
# Reorder columns to match your CREATE TABLE statement
final_df = df[[
    'date_key', 'full_date', 'year', 'quarter', 'month', 'month_name',
    'day', 'day_of_week', 'day_of_week_name', 'markets_open',
    'markets_closed', 'holidays'
]]

# The to_json function needs to serialize date objects, so let's keep them as strings
final_df['full_date'] = final_df['full_date'].dt.strftime('%Y-%m-%d')

# Convert the stringified JSON back into actual lists/dicts for the final output
final_df['markets_open'] = final_df['markets_open'].apply(json.loads)
final_df['markets_closed'] = final_df['markets_closed'].apply(json.loads)
final_df['holidays'] = final_df['holidays'].apply(json.loads)

# Save as Newline Delimited JSON
final_df.to_json(
    OUTPUT_FILE,
    orient='records',
    lines=True
)

print(f"✅ Done! NDJSON file {OUTPUT_FILE} created successfully.")