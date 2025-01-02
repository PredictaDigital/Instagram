import requests
import json
import pyodbc

# Instagram API endpoint
API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '17841448939781055'
ACCESS_TOKEN = 'EAAFe1V0tVGYBOwrUqiEzDfRCiO0lK7tZBHJYZA5qWUgmZCJvM0RTlYVBLurtwnbrb8DlYD7LD1ZBVZAMSaBo13O2k6ObwGk3GsWUzfjvTZCG89ZBeNPM5rZC26AcKUjzJp0xBZBPQuDUwUe65ocx0TWyyAWA4VtwqtU8yPTGQo5f1tZCTnK5LeUY9y9Nz2EmJZAVjUjWWE1TfkDLZBzZAZChvBPpVxDEGmo0LSPfFmZAAZDZD'

endpoint = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/insights"
# Define the parameters for the request 17841448939781055
params = {
    'access_token': ACCESS_TOKEN,
    'metric': 'audience_country',
    'period': 'lifetime'
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
# print(analytics_data)
#----------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Extract the insights data
insights_data = analytics_data.get('data', [])
# Loop through each entry in the insights data and insert it into the SQL table
for entry in insights_data:
     metric_name = entry.get('name', '')
     values = entry.get('values', [])
# print(values)
    # Loop through the values and insert them into the SQL table
for value in values:
    value_data = value.get('value', {})
# print(value_data)

# Loop through the value data (city-value pairs) and insert them into the SQL table
for country_code, value in value_data.items():
# Define the SQL query to insert the data into the table
    insert_query = ("INSERT INTO dbo.Instagram_Followers_By_Country_KS (PageID, MetricName, Country_Code, Value)"
                            "VALUES (?, ?, ?, ?)")
    # Execute the SQL query
    cursor.execute(insert_query, ("17841448939781055", metric_name, country_code, value))
    conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
