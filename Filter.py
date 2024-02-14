import tkinter as tk
from tkinter import filedialog
import pandas as pd
import re
from datetime import datetime
from IPython.display import display

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
        ce_df = df[df['Ticker'].str.endswith('CE.NFO')]
        if not ce_df.empty:
            ce_df = ce_df.sort_values(by=['parsed_date', 'Time'], ascending=True)
            print("Data for CE rows:")

            # Define the start time for comparison
            start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
            end_time = datetime.strptime('09:15:59', '%H:%M:%S').time()

            nearest_value = None
            nearest_index = None
            for index, row in ce_df.iterrows():
                ce_time = row['Time']
                # Check if the time is within the range of 9:15:00 to 9:15:59
                if start_time <= ce_time <= end_time:
                    close_value = row['Close']
                    if close_value < 200:
                        # Update the nearest value if this is the first found or closer to 200 than previous finds
                        if nearest_value is None or abs(close_value - 200) < abs(nearest_value - 200):
                            nearest_value = close_value
                            nearest_index = index
                elif ce_time > end_time:
                    # Break the loop if the time is past 9:15:59
                    break

            if nearest_index is not None:
                print(f"Nearest value less than 200 found at index {nearest_index}: {nearest_value}")
                print("CE rows starting from the nearest value:")
                ce_rows_to_print = ce_df.loc[nearest_index:]
                print(ce_rows_to_print.to_string(index=False))
            else:
                print("No value less than 200 found between 9:15:00 and 9:15:59.")
        else:
            print("No CE rows found.")
    else:
        print("DataFrame is missing one or more required columns.")




# Function to handle file selection
def select_file():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(title="Select CSV File", filetypes=[("CSV files", "*.csv")])
    if file_path:
        process_csv(file_path)
    else:
        print("No file selected.")

# Call the file selection function
select_file()
