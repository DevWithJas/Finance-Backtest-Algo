import tkinter as tk
from tkinter import filedialog
import pandas as pd
import re
from datetime import datetime
import os
from concurrent.futures import ProcessPoolExecutor

def parse_date_from_ce_ticker(ticker):
    date_pattern = re.compile(r"BANKNIFTY(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})")
    match = date_pattern.search(ticker)
    if match:
        day = int(match.group(1))
        month = match.group(2)
        year = int('20' + match.group(3))
        date_str = f"{day}-{month}-{year}"
        return datetime.strptime(date_str, "%d-%b-%Y").date()
    return None

def process_data_for_date(df):
    ce_df = df[df['Ticker'].str.endswith('CE.NFO')].copy()
    if ce_df.empty:
        return pd.DataFrame()  # Return empty DataFrame for consistency
        
    ce_df.sort_values(by=['Time'], ascending=True, inplace=True)
    start_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
    end_time = datetime.strptime('09:15:59', '%H:%M:%S').time()

    filtered_df = ce_df[(ce_df['Time'] >= start_time) & (ce_df['Time'] <= end_time) & (ce_df['Close'] < 200)]

    if filtered_df.empty:
        return pd.DataFrame()  # Return empty DataFrame for consistency

    filtered_df = filtered_df.assign(difference=(200 - filtered_df['Close']).abs())
    nearest_row = filtered_df.loc[filtered_df['difference'].idxmin()]

    return ce_df.loc[nearest_row.name:]

def process_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        return pd.DataFrame(), f"Failed to read {csv_path}: {e}"

    if not {'Ticker', 'Close', 'Time'}.issubset(df.columns):
        return pd.DataFrame(), "DataFrame is missing one or more required columns."

    df['parsed_date'] = df['Ticker'].apply(parse_date_from_ce_ticker)
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.time
    unique_dates = df['parsed_date'].dropna().unique()

    output_dataframes = [process_data_for_date(df[df['parsed_date'] == date]) for date in unique_dates]
    final_df = pd.concat(output_dataframes)

    if final_df.empty:
        return pd.DataFrame(), "No data matching criteria found."

    return final_df, "Success"

def detailed_analysis(final_dataframe):
    if final_dataframe.empty:
        return 0  # No data to analyze.
    final_dataframe['Time'] = pd.to_datetime(final_dataframe['Time'], format='%H:%M:%S').dt.time
    df_sorted = final_dataframe.sort_values(by='Time')
    target_time_start = pd.to_datetime('09:30:00').time()
    specific_time_index = df_sorted[df_sorted['Time'] >= target_time_start].index.min()
    initial_target_index = df_sorted.loc[specific_time_index:][df_sorted['Close'] > 250].index.min()
    initial_target_value = df_sorted.loc[initial_target_index, 'Close'] if pd.notnull(initial_target_index) else None
    df_sorted['Target Value'] = pd.NA
    df_sorted['Sell_Buy_CutPosition'] = None
    df_sorted['Mark'] = None
    df_sorted['Loss'] = pd.NA
    df_sorted['Difference'] = None
    if pd.notnull(initial_target_index):
        current_target_value = initial_target_value
        target_value_updated = False  
        for i in df_sorted.loc[initial_target_index:].index:
            row_close = df_sorted.at[i, 'Close']
            if row_close > current_target_value:
                if not target_value_updated:
                    df_sorted.at[i, 'Sell_Buy_CutPosition'] = 'Dummy'
                    target_value_updated = True 
                else:
                    df_sorted.at[i, 'Sell_Buy_CutPosition'] = 'Cut Position'
                df_sorted.at[i, 'Loss'] = row_close - current_target_value
                current_target_value = row_close
                df_sorted.at[i, 'Target Value'] = current_target_value
            else:
                if not target_value_updated and i > initial_target_index:
                    df_sorted.at[i, 'Sell_Buy_CutPosition'] = 'Dummy'
                    target_value_updated = True  
                df_sorted.at[i, 'Target Value'] = current_target_value

    def mark_930_and_1515(df):
        for idx, row in df.iterrows():
            if row['Time'] == pd.to_datetime('09:30:00').time():
                df.at[idx, 'Mark'] = 'N'
            elif row['Time'] == pd.to_datetime('15:15:00').time():
                df.at[idx, 'Mark'] = 'C'
        return df

    df_sorted = mark_930_and_1515(df_sorted)

    previous_mark = None
    def mark_entries_with_conditions(df):
        for idx in range(len(df)):  
            current_position = df.iloc[idx]['Sell_Buy_CutPosition']
            current_time = df.iloc[idx]['Time']
            if current_time >= pd.to_datetime('15:15:00').time() and current_time < pd.to_datetime('15:16:00').time():
                df.at[df.index[idx], 'Mark'] = 'C'
                break  
            if current_position == 'Cut Position':
                df.at[df.index[idx], 'Mark'] = 'C'
                if idx + 1 < len(df): 
                    next_time = df.iloc[idx + 1]['Time']
                    if next_time < pd.to_datetime('15:15:00').time() and df.iloc[idx + 1]['Mark'] is None:
                        df.at[df.index[idx + 1], 'Mark'] = 'N'
            elif current_position == 'Dummy':
                df.at[df.index[idx], 'Mark'] = 'N'
        return df

    df_sorted = mark_entries_with_conditions(df_sorted)

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
    
    return total_difference

def process_file(file_path):
    final_df, message = process_csv(file_path)
    if not final_df.empty:
        return detailed_analysis(final_df)
    else:
        return 0

def select_folder():
    root = tk.Tk()
    root.withdraw()
    folder_path = filedialog.askdirectory(title="Select Folder Containing CSV Files")
    root.destroy()
    return folder_path

def process_files_in_folder(folder_path):
    if not os.path.isdir(folder_path):
        return "Invalid folder path."

    total_difference = 0
    with ProcessPoolExecutor() as executor:
        for root, dirs, files in os.walk(folder_path):
            if root != folder_path:  # Skip the main folder
                print("Processing subfolder:", root)
                for file in files:
                    if file.endswith('.csv'):
                        file_path = os.path.join(root, file)
                        difference = process_file(file_path)
                        total_difference += difference
                        print(f"Processed file: {file_path}, Difference: {difference}")

    return total_difference


if __name__ == '__main__':
    folder_path = select_folder()
    if folder_path:
        total_difference = process_files_in_folder(folder_path)
        print("Total Difference:", total_difference)
    else:
        print("No folder selected.")
