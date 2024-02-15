import tkinter as tk
from tkinter import filedialog
import pandas as pd
import re
from datetime import datetime

def parse_date_from_ce_ticker(ticker):
    date_pattern = re.compile(r"BANKNIFTY(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})")
    match = date_pattern.search(ticker)
    if match:
        day = int(match.group(1))
        month = match.group(2)
        year = int('20' + match.group(3))  # Ensure the year is correctly prefixed with '20'
        date_str = f"{day}-{month}-{year}"
        try:
            parsed_date = datetime.strptime(date_str, "%d-%b-%Y").date()
            return parsed_date
        except ValueError as e:
            print(f"Error parsing date from {ticker}: {e}")
            return None
    else:
        print(f"No date match found for {ticker}")
        return None

def process_csv(csv_path):
    print(f"Processing file: {csv_path}")
    df = pd.read_csv(csv_path)
    if {'Ticker', 'Close', 'Time'}.issubset(df.columns):
        df['parsed_date'] = df['Ticker'].apply(parse_date_from_ce_ticker)
        df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.time
        unique_dates = sorted(df['parsed_date'].dropna().unique())

        if unique_dates:
            output_dataframes = []  # List to store output DataFrames for this CSV file
            for date in unique_dates:
                df_date = df[df['parsed_date'] == date]
                if not df_date.empty:
                    print(f"Processing for date: {date}")
                    filtered_df = process_data_for_date(df_date)
                    if filtered_df is not None:
                        output_dataframes.append(filtered_df)

            if output_dataframes:
                final_df = pd.concat(output_dataframes)
                print("\nFinal DataFrame:")
                print(final_df)
                return final_df  # Return the final DataFrame
            else:
                print("No output data to concatenate.")
                return None
        else:
            print("No valid dates found in the CSV.")
            return None
    else:
        print("DataFrame is missing one or more required columns.")
        return None

def process_data_for_date(df):
    ce_df = df[df['Ticker'].str.endswith('CE.NFO')].copy()  # Use .copy() to avoid SettingWithCopyWarning
    if ce_df.empty:
        return None  # Return None if ce_df is empty
        
    ce_df = ce_df.sort_values(by=['Time'], ascending=True)
    start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
    end_time = datetime.strptime('09:15:59', '%H:%M:%S').time()

    filtered_df = ce_df[(ce_df['Time'] >= start_time) & (ce_df['Time'] <= end_time) & (ce_df['Close'] < 200)]

    if filtered_df.empty:
        return None  # Return None if filtered_df is empty

    filtered_df.loc[:, 'difference'] = (200 - filtered_df['Close']).abs()

    nearest_row = filtered_df.loc[filtered_df['difference'].idxmin()]

    return ce_df.loc[nearest_row.name:]

# Function to select file and start processing
def select_file():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title="Select CSV File", filetypes=[("CSV files", "*.csv")])
    if file_path:
        return process_csv(file_path)  # Return the final DataFrame
    else:
        print("No file selected.")
        return None

# Call the file selection function
final_dataframe = select_file()

# Now you can use 'final_dataframe' directly in your other code without saving to Excel first
import pandas as pd

# Convert 'Time' column to datetime.time for accurate comparison
final_dataframe['Time'] = pd.to_datetime(final_dataframe['Time'], format='%H:%M:%S').dt.time

# Sort the DataFrame by 'Time' to ensure chronological processing
df_sorted = final_dataframe.sort_values(by='Time')

# Find the index of the first entry at or after 9:30:00
target_time_start = pd.to_datetime('09:30:00').time()
specific_time_index = df_sorted[df_sorted['Time'] >= target_time_start].index.min()

# Find initial target index and value for 'Close' values greater than 250, starting from the specific time index
initial_target_index = df_sorted.loc[specific_time_index:][df_sorted['Close'] > 250].index.min()
initial_target_value = df_sorted.loc[initial_target_index, 'Close'] if pd.notnull(initial_target_index) else None

# Initialize additional columns
df_sorted['Target Value'] = pd.NA
df_sorted['Sell_Buy_CutPosition'] = None
df_sorted['Mark'] = None
df_sorted['Loss'] = pd.NA
df_sorted['Difference'] = None

# Assuming initial_target_index and initial_target_value have been identified
if pd.notnull(initial_target_index):
    current_target_value = initial_target_value
    target_value_updated = False  # Flag to indicate when the target value is first updated

    # Loop to update target values and other columns starting from the initial target index
    for i in df_sorted.loc[initial_target_index:].index:
        row_close = df_sorted.at[i, 'Close']
        
        if row_close > current_target_value:
            if not target_value_updated:
                # Mark as 'Dummy' only for the first update to the target value
                df_sorted.at[i, 'Sell_Buy_CutPosition'] = 'Dummy'
                target_value_updated = True  # Ensure we only mark the first update
            else:
                df_sorted.at[i, 'Sell_Buy_CutPosition'] = 'Cut Position'
            
            # Update the target value since a new higher close value is found
            df_sorted.at[i, 'Loss'] = row_close - current_target_value
            current_target_value = row_close
            df_sorted.at[i, 'Target Value'] = current_target_value
        else:
            if not target_value_updated and i > initial_target_index:
                # If target value hasn't been updated yet, mark the next applicable row as 'Dummy'
                # This ensures the marking happens only if the condition is met after the initial finding
                df_sorted.at[i, 'Sell_Buy_CutPosition'] = 'Dummy'
                target_value_updated = True  # Update the flag after marking
            # Even if the condition isn't met, ensure the target value is carried forward
            df_sorted.at[i, 'Target Value'] = current_target_value

# Define function to mark 9:30 and 15:15 times
def mark_930_and_1515(df):
    for idx, row in df.iterrows():
        if row['Time'] == pd.to_datetime('09:30:00').time():
            df.at[idx, 'Mark'] = 'N'
        elif row['Time'] == pd.to_datetime('15:15:00').time():
            df.at[idx, 'Mark'] = 'C'
    return df

# Initially apply the time-based marking function
df_sorted = mark_930_and_1515(df_sorted)

# Revised logic to handle 'Cut Position' and 'Dummy' marking with 'N'
previous_mark = None
# Iterate through df_sorted to apply logic for 'Cut Position', 'Dummy', and the subsequent 'N' marking
import pandas as pd

def mark_entries_with_conditions(df):
    for idx in range(len(df)):  # Ensure we cover the last row as well
        current_position = df.iloc[idx]['Sell_Buy_CutPosition']
        current_time = df.iloc[idx]['Time']
        
        # Adjust condition to check for "15:15" minute
        if current_time >= pd.to_datetime('15:15:00').time() and current_time < pd.to_datetime('15:16:00').time():
            df.at[df.index[idx], 'Mark'] = 'C'
            break  # Stop processing as we've marked the first occurrence within "15:15" as 'Cut Position'

        # Handle 'Cut Position' and 'Dummy' marking
        if current_position == 'Cut Position':
            df.at[df.index[idx], 'Mark'] = 'C'
            # Ensure the next row is marked as 'N', only if we haven't reached the "15:15" time
            if idx + 1 < len(df):  # Ensure we do not go out of bounds
                next_time = df.iloc[idx + 1]['Time']
                if next_time < pd.to_datetime('15:15:00').time() and df.iloc[idx + 1]['Mark'] is None:
                    df.at[df.index[idx + 1], 'Mark'] = 'N'
        elif current_position == 'Dummy':
            # Directly mark the 'Dummy' row as 'N'
            df.at[df.index[idx], 'Mark'] = 'N'

    return df

# Re-apply the marking logic
df_sorted = mark_entries_with_conditions(df_sorted)

# Calculate difference and sum it up
df_sorted['Difference'] = None
last_n_close = None
last_n_index = None

for index, row in df_sorted.iterrows():
    if row['Time'] == '3:15':
        break
    if row['Mark'] == 'N':
        last_n_close = row['Close']
        last_n_index = index
    elif row['Mark'] == 'C' and last_n_close is not None:
        diff = last_n_close - row['Close']
        df_sorted.at[index, 'Difference'] = diff
        last_n_close = None
        last_n_index = None

total_difference = df_sorted['Difference'].sum()

# Set maximum number of rows displayed to None (show all rows)
pd.set_option('display.max_rows', None)

# Define columns to display
display_cols = ['Time', 'Close', 'Target Value', 'Sell_Buy_CutPosition', 'Difference', 'Mark']

# Display selected columns
print(df_sorted[display_cols])

# Display total difference
print("Total Difference:", total_difference)
