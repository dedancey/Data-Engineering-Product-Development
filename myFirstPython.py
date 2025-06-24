import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import urllib

def drop_na_rows(df):
    return df.dropna()

def removeDuplicates(df):
    return df.drop_duplicates()

def drop_na_subset(df):
    return df.dropna(subset=["Customer ID","Customer Name"], inplace=True)

#def fileLoader(df):

def resetIndex(df):
    return df.reset_index()


def remove_all_quotes(df):
    return df.applymap(
        lambda x: x.replace('"', '') if isinstance(x, str) else x
    )

def convert_to_datetime(df, columns, date_format="%d/%m/%Y"):
    for col in columns:
        df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
    return df

def add_days_on_loan(df):
    # Ensure the dates are datetime
    df = convert_to_datetime(df, ["Book checkout", "Book Returned"])
    
    # Calculate DaysOnLoan
    df["DaysOnLoan"] = (df["Book Returned"] - df["Book checkout"]).dt.days
    
    return df

def clean_text_values(df):
    return df.astype(str).apply(lambda col: col.str.strip().str.replace('"', '', regex=False))
    

def clean_systembook(df):
    df = clean_text_values(df)
    df = convert_to_datetime(df, ["Book checkout", "Book Returned"])
    df = add_days_on_loan(df)
    df = removeDuplicates(df)
    df = resetIndex(df)
    return df

def clean_systemcustomers(df):
    df = drop_na_rows(df)
    df = removeDuplicates(df)
    df = resetIndex(df)
    return df

def add_days_on_loan(df):
    # Ensure the dates are datetime
    df = convert_to_datetime(df, ["Book checkout", "Book Returned"])
    # Calculate DaysOnLoan
    df["DaysOnLoan"] = (df["Book Returned"] - df["Book checkout"]).dt.days
    return df


def main():
    Systembook = pd.read_csv(r"C:\Users\Admin\Desktop\Data-Engineering-Product-Development\python_app\03_Library Systembook.csv")
    SystemCustomers = pd.read_csv(r"C:\Users\Admin\Desktop\Data-Engineering-Product-Development\python_app\03_Library SystemCustomers.csv")

    cleaned_books = clean_systembook(Systembook)
    cleaned_customers = clean_systemcustomers(SystemCustomers)

    output_folder = r"C:\Users\Admin\Desktop\Data-Engineering-Product-Development\python_app"
    file_path_1 = os.path.join(output_folder, "03_Library Systembook_Cleaned.csv")
    file_path_2 = os.path.join(output_folder, "03_library System_Customers_Cleaned.csv")

    cleaned_books.to_csv(file_path_1, index=False)
    cleaned_customers.to_csv(file_path_2, index=False)

    # SQL Server connection
    server = "localhost"
    database = "python_app"
    driver = "ODBC Driver 17 for SQL Server"
    params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=python_app;Trusted_Connection=yes;"
)
    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(connection_string)

    # Load cleaned CSVs and push to SQL
    df_books = pd.read_csv(file_path_1)
    df_customers = pd.read_csv(file_path_2)

    df_customers.to_sql("SystemCustomers", con=engine, if_exists='replace', index=False)
    df_books.to_sql("SystemBook", con=engine, if_exists='replace', index=False)

# This ensures main() only runs when script is executed directly
if __name__ == '__main__':
    main()