import requests
import json
import pyodbc
from datetime import datetime, timedelta

# Facebook API endpoint
API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '17841448939781055'
ACCESS_TOKEN = 'EAAFe1V0tVGYBOzqjcDxeZCdhiRpaa9urBNuKhs7G2sougyk3Acuj0exQe1r52W0YGkOwjYtuZC10OfwF6GX3rEJ7JM9R5jeZBWfzEdpkz2E53JIGWKfhOhFkkDzCkSQxBoIS69lEZCv0JqH4hHblkXQsOsHiNXyXZB82HMCATAp5RZB1pJcJmAaZCBEUGX6fOYZCkU69PHW6wCzoEZAAZD'

# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
Instagram_Table = "dbo.Instagram_Page_Insights_KS"      #need to change this to Fact table
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Get the maximum date present in the table
cursor.execute(f"SELECT MAX([end_time]) FROM {Instagram_Table}")
max_date = cursor.fetchone()[0]
# print(max_date)
# Convert max_date to datetime object if it's not None
if max_date is not None:
    max_date = datetime.now().date()
else:
    max_date = datetime.now().date() - timedelta(days=29)  # If no data exists, start from yesterday

since_date = max_date - timedelta(days=1)
until_date = datetime.now().date()
# since_date = '2024-04-28'
# until_date = '2024-05-27'
# delete_date = max_date -timedelta(days=2)

endpoint = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/insights"
# Define the parameters for the request 17841448939781055
params = {
    'access_token': ACCESS_TOKEN,
    # 'fields': 'id,ig_id, follows',
    'metric': 'follower_count,impressions,reach,email_contacts,phone_call_clicks,text_message_clicks,get_directions_clicks,'
              'website_clicks,profile_views',
    'since': since_date,
    'until': until_date,
    'period': 'day'
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
list_type_data = analytics_data.get('data')
# print(list_type_data)
#----------------------------------------------------------------------------------------------
# Delete existing data for the last two days
# delete_query = f"DELETE FROM {Instagram_Table} WHERE [end_time] >?"
# cursor = conn.cursor()
# cursor.execute(delete_query, ({delete_date}))
# cursor.close()

# Insert new data in SQL table
result_data = {}
for item in list_type_data:
    metric_name = item.get("name")
    for value in item.get("values"):
        if value.get('end_time') in result_data:
            result_data[value.get('end_time')].update({metric_name: value.get('value')})
        else:
            result_data[value.get('end_time')] = {metric_name: value.get('value')}

# print(result_data)
for end_time, metrics in result_data.items():
    # Constructing the INSERT query
    query = f'''
        INSERT INTO dbo.Instagram_Page_Insights_KS (end_time,follower_count,impressions,reach,email_contacts,
                phone_call_clicks,text_message_clicks,get_directions_clicks,website_clicks,profile_views)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
# Executing the INSERT query
    cursor.execute(query, (end_time,metrics['follower_count'],metrics['impressions'],metrics['reach'],
                           metrics['email_contacts'],metrics['phone_call_clicks'],metrics['text_message_clicks'],
                           metrics['get_directions_clicks'],metrics['website_clicks'],metrics['profile_views']))
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()