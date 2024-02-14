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
            for date in unique_dates:
                print(f"\nProcessing for date: {date}")
                df_date = df[df['parsed_date'] == date]
                process_data_for_date(df_date)
        else:
            print("No valid dates found in the CSV.")
    else:
        print("DataFrame is missing one or more required columns.")

def process_data_for_date(df):
    ce_df = df[df['Ticker'].str.endswith('CE.NFO')]
    if not ce_df.empty:
        ce_df = ce_df.sort_values(by=['Time'], ascending=True)
        print(f"\nData for CE rows on the selected date:")
        start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        end_time = datetime.strptime('09:15:59', '%H:%M:%S').time()

        filtered_df = ce_df[(ce_df['Time'] >= start_time) & (ce_df['Time'] <= end_time) & (ce_df['Close'] < 200)]

        if not filtered_df.empty:
            # Find the nearest value to 200 within the time range
            filtered_df['difference'] = (200 - filtered_df['Close']).abs()
            nearest_row = filtered_df.loc[filtered_df['difference'].idxmin()]

            print("Nearest value to 200 found:")
            print(nearest_row.to_frame().T.drop(columns=['difference']).to_string(index=False))
            
            # Displaying full data after the nearest value found
            print("\nFull data from the nearest value to the end of the selected time range:")
            after_nearest_df = ce_df.loc[nearest_row.name:]
            print(after_nearest_df.to_string(index=False))
        else:
            print("No value less than 200 found between 9:15:00 and 9:15:59.")
    else:
        print("No CE rows found for the selected date.")

def select_file():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title="Select CSV File", filetypes=[("CSV files", "*.csv")])
    if file_path:
        process_csv(file_path)
    else:
        print("No file selected.")

# Call the file selection function
select_file()
