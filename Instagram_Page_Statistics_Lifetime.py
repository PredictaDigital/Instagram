import requests
import pyodbc

# Facebook API endpoint
API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '17841448939781055'
ACCESS_TOKEN = 'EAAFe1V0tVGYBO74mh9jnZC6OI7UD5UuB5jmCJfibkZBFOnX9dNwHTbW6ClsHGJHSrY5gKBAun7nsfUMvImj0mVjUwmQBK466CIKa3ut2hhqa8d1BnUV8aOcxHbxyWoMEJrDUWCb5an9KdmEriOPob6DTDpZBJEhY31tJlhpyhBEaSmjpROJCZBZBNrKAmcxd8uX9ZAynOQvdBLhiUV5mo2PaiZAmOG4UjRZCfsZCjD2X6'
endpoint = fhttpsgraph.facebook.com{API_VERSION}{PAGE_ID}
# Define the parameters for the request 17841448939781055
params = {
    'access_token' ACCESS_TOKEN,
    'fields' 'id, name,username,followers_count,follows_count,media_count,website,profile_picture_url,biography'
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
# print(analytics_data)

#---------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43&^FHHytf'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()
# Truncate the table
cursor.execute('TRUNCATE TABLE dbo.Instagram_Page_Statistics_Lifetime_KS')

sql_query = '''INSERT INTO dbo.Instagram_Page_Statistics_Lifetime_KS (id, name, username, followers_count, follows_count, media_count, website, profile_picture_url,biography)
               VALUES (, , , , , , , , )'''

# Extract relevant data from the analytics_data
data = (analytics_data['id'], analytics_data['name'], analytics_data['username'],
        analytics_data['followers_count'], analytics_data['follows_count'],
        analytics_data['media_count'], analytics_data['website'],
        analytics_data['profile_picture_url'], analytics_data['biography'])

# Execute the SQL query
cursor.execute(sql_query, data)

conn.commit()
# Close the cursor and connection
cursor.close()
conn.close()
