#importing  required library
import gspread
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine

#Fetching the data
def fetch_google_sheet (scope, json_path,google_sheet_name ):
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open(google_sheet_name).sheet1  # or use .get_worksheet(index) for a specific sheet
    df = pd.DataFrame(sheet.get_all_records())
    return df


#CLEANING PART
def clean_data(df):
    # Drop rows with missing values
    df.dropna(inplace=True)
    
    # Drop duplicate rows
    df.drop_duplicates(inplace=True)

    # round by 2 decimal
    df = df.apply(lambda x: x.round(2) if x.dtype == 'float' else x)

    
    # Drop the 'Postal Code' column if it exists
    if "Postal Code" in df.columns:
        df.drop(columns=["Postal Code"], inplace=True)
    
    # Convert 'Order Date' and 'Ship Date' columns to datetime format
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce')
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors='coerce')
    
    # Remove extra spaces from all text columns
    for col in df.columns:
        if df[col].dtype == 'object':  # Checks if the column is text
            df[col] = df[col].str.strip()
    
    return df

# TRANFERED PYTHON TO SQL

def python_to_sql (username, password, host, database, table_name, df):
    engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}/{database}")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print("Data inserted successfully!")






