#1 IMPORTING REQUIRED LIBRARIES
import gspread
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from oauth2client.service_account import ServiceAccountCredentials
import preprocessor 
from sqlalchemy import create_engine


#DEFINE CREDENTIAL
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
json_path =  "C:/Users/vbaro/OneDrive/Desktop/MASAI/project 29-10/Global Super Store Dataset/crimedata-434001-7a6dad82a010.json"
google_sheet_name =  "Global_Superstore2"
df = preprocessor.fetch_google_sheet(scope, json_path, google_sheet_name)

# CLEANING PART
df =preprocessor.clean_data(df)
print(df.info())


# TRANFERED PYTHON TO SQL
username = "Vivek443362"
password = "Vivek%402000"
host = "localhost"
database = "project"
table_name = "salesdata"

preprocessor.python_to_sql (username, password, host, database, table_name,df)