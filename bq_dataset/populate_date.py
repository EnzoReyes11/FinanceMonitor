import pandas as pd
import holidays
import datetime

print("🚀 Starting date dimension generation...")

# --- 1. Configuration ---
# Define the date range for your dimension table.
# A long range is good so you don't have to update it often.
START_YEAR = 2015
END_YEAR = 2040

# Define the markets you want to track.
# The key is the code you want to use (e.g., 'USA').
# The value is the object from the 'holidays' library.
# This makes it very easy to add new markets in the future!
MARKETS = {
    'US': holidays.US(),
    'AR': holidays.AR()
    # To add Great Britain, you would just add:
    # 'GBR': holidays.GB()
}

# Output file name
OUTPUT_CSV_FILE = 'dim_date.csv'


# --- 2. Generate Date Range ---
print(f"🗓️  Generating dates from {START_YEAR} to {END_YEAR}...")
start_date = datetime.date(START_YEAR, 1, 1)
end_date = datetime.date(END_YEAR, 12, 31)
df = pd.DataFrame({'full_date': pd.date_range(start_date, end_date)})


# --- 3. Create Standard Date Columns ---
print("⚙️  Calculating standard date attributes...")
df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
df['year'] = df['full_date'].dt.year
df['quarter'] = df['full_date'].dt.quarter
df['month'] = df['full_date'].dt.month
df['month_name'] = df['full_date'].dt.strftime('%B')
df['day'] = df['full_date'].dt.day
df['day_of_week_name'] = df['full_date'].dt.strftime('%A')
df['is_weekend'] = df['full_date'].dt.dayofweek.isin([5, 6])


# --- 4. Calculate Holiday Markets (The Core Logic) ---
print("🏖️  Calculating holidays for markets:", ", ".join(MARKETS.keys()))

# This function checks a single date against all defined markets.
def get_holiday_markets(date):
    markets_on_holiday = []
    for market_code, holiday_calendar in MARKETS.items():
        if date in holiday_calendar:
            markets_on_holiday.append(market_code)
    return markets_on_holiday

# Apply the function to each date in the DataFrame.
df['holiday_markets'] = df['full_date'].apply(get_holiday_markets)


# --- 5. Final Formatting and Export ---
print("📄 Formatting for BigQuery and exporting...")

# The BigQuery CSV loader works best if the array is a simple
# delimited string. We'll use a pipe '|' as the delimiter.
# e.g., ['US', 'AR'] becomes "US|AR".
# We will convert this back to an ARRAY type inside BigQuery.
df['holiday_markets'] = df['holiday_markets'].apply(lambda x: '|'.join(x))

# Ensure columns are in the correct order for the BQ schema.
final_columns = [
    'date_key',
    'full_date',
    'year',
    'quarter',
    'month',
    'month_name',
    'day',
    'day_of_week_name',
    'is_weekend',
    'holiday_markets' # This will be loaded as a STRING
]
df = df[final_columns]

# Convert 'full_date' to a string in the required format YYYY-MM-DD
df['full_date'] = df['full_date'].dt.strftime('%Y-%m-%d')


# Export to CSV
df.to_csv(OUTPUT_CSV_FILE, index=False)

print(f"\n🎉 Success! Dimension table created at '{OUTPUT_CSV_FILE}'.")
print(f"Total rows generated: {len(df)}")