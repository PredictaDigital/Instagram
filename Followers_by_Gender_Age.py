import requests
import json
import pyodbc

# Facebook API endpoint
GRAPH_API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '17841448939781055'
ACCESS_TOKEN = 'EAAFe1V0tVGYBO1PJo2hEwEK4v3eOherLHDTpnUxhyMhhr15VZBOZAZBZCjXz5lmCyQJxRCwSCbfZBWLYm5K4gPtAWXZChHweRKHX95bWg8fZCaRdxZAZCK7Uo6ZA6ARon0VizH3yRNhmPTcK5S2bF5UoTYF7eofSUgy9CCBBBDTBPEYPfv4ZCzeHaUl4V0coqXM6BucJUPxZB2JCR1hMKWYZD'

endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{PAGE_ID}/media"
# Define the parameters for the request 17841448939781055
params = {
    'access_token': ACCESS_TOKEN,
    'fields': 'id,ig_id,timestamp,media_type,comments_count,like_count,permalink,username,caption,media_product_type,'
              'is_comment_enabled,media_url,insights.metric(impressions,reach,profile_visits,profile_activity,replies,'
              'saved,video_views,shares,total_interactions,follows)'
}
#exits,engagement,
response = requests.get(endpoint, params=params)

analytics_data = response.json()
list_type_data = analytics_data.get('data')
print(list_type_data)
#----------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

result_data = {}
for item in list_type_data:
    insights = item.get('insights', {}).get('data', [])
    impressions = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'impressions'), 0)
    reach = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'reach'), 0)
    profile_visits = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'profile_visits'),
                        0)
    profile_activity = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'profile_activity'), 0)
    replies = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'replies'), 0)
    saved = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'saved'), 0)
    video_views = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'video_views'),
                       0)
    shares = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'shares'), 0)
    total_interactions = next(
        (insight['values'][0]['value'] for insight in insights if insight['name'] == 'total_interactions'), 0)
    follows = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'follows'), 0)

    query = f'''
        INSERT INTO dbo.Instagram_Media_Insights_KS (id, ig_id,post_date,media_type,comments_count,like_count,permalink,
                      username,caption,media_product_type,is_comment_enabled,media_url,impressions,reach,profile_visits,
                      profile_activity,replies,saved,video_views,shares,total_interactions,follows)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    cursor.execute(query, (item.get("id"),item.get("ig_id"),item.get("timestamp"),item.get("media_type"),
                           item.get("comments_count"),item.get("like_count"),item.get("permalink"),item.get("username"),
                           item.get("caption"),item.get("media_product_type"),item.get("is_comment_enabled"),
                           item.get("media_url"),impressions,reach,profile_visits,profile_activity,replies,saved,
                           video_views,shares,total_interactions,follows))
conn.commit()
conn.close()