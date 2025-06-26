import requests
import pandas as pd

# Define the API endpoint
url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'  # Replace with your API's URL

response = requests.get(url)

if response.status_code == 200:
    # Parse the JSON data
    data = response.json()
    
    # Assuming the data is in the correct format (list of dicts)
    df = pd.DataFrame(data)
    print(df.head())  # Display the first few rows of the DataFrame
else:
    print(f"Error: Status Code {response.status_code}. Response: {response.text}")

def convert_to_datetime(df, columns, date_format="%d/%m/%Y"):
    for col in columns:
        df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
    return df

date_columns = ["last_updated","atl_date","atl_change_percentage","ath_date"]
convert_dates = convert_to_datetime(df, date_columns)
