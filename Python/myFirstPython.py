import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import urllib

def drop_na_subset(df):
    return df.replace(["", "nan", "NaN", "NULL"], np.nan)

def drop_na_rows(df, name, drop_log):
    original_len = len(df)
    df1 = drop_na_subset(df)
    df_cleaned = df1.dropna()
    dropped = original_len - len(df_cleaned)
    drop_log.append({"DataFrame": name, "Step": "Drop NA Rows", "RowsRemoved": dropped})
    return df_cleaned

def removeDuplicates(df, name, drop_log):
    original_len = len(df)
    df_cleaned = df.drop_duplicates()
    duplicates_removed = original_len - len(df_cleaned)
    drop_log.append({"DataFrame": name, "Step": "Remove Duplicates", "RowsRemoved": duplicates_removed})
    return df_cleaned

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

""" def summary(step, output, table_name=""):
    print(output)
    data_summary.append({"step": step, "output": output, "table_name": table_name}) """

def clean_systembook(df, drop_log):
    df = clean_text_values(df)
    df = convert_to_datetime(df, ["Book checkout", "Book Returned"])
    df = add_days_on_loan(df)
    df = removeDuplicates(df, "Systembook", drop_log)
    df = drop_na_rows(df, "Systembook", drop_log)
    df = resetIndex(df)
    return df

def clean_systemcustomers(df, drop_log):
    df = drop_na_rows(df, "SystemCustomers", drop_log)
    df = removeDuplicates(df, "SystemCustomers", drop_log)
    df = resetIndex(df)
    return df

def add_days_on_loan(df):
    # Ensure the dates are datetime
    df = convert_to_datetime(df, ["Book checkout", "Book Returned"])
    # Calculate DaysOnLoan
    df["DaysOnLoan"] = (df["Book Returned"] - df["Book checkout"]).dt.days
    return df


def main():
    drop_log = []
    Systembook = pd.read_csv("03_Library Systembook.csv")
    SystemCustomers = pd.read_csv("03_Library SystemCustomers.csv")

    cleaned_books = clean_systembook(Systembook, drop_log)
    cleaned_customers = clean_systemcustomers(SystemCustomers, drop_log)

    output_folder = "."
    file_path_1 = os.path.join(output_folder, "03_Library Systembook_Cleaned.csv")
    file_path_2 = os.path.join(output_folder, "03_library System_Customers_Cleaned.csv")
    drop_log_file = os.path.join(output_folder, "Dropped_Rows_Summary.csv")

    cleaned_books.to_csv(file_path_1, index=False)
    cleaned_customers.to_csv(file_path_2, index=False)
    pd.DataFrame(drop_log).to_csv(drop_log_file, index=False)

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