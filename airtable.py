"""
Interfaces with Airtable to sync local CSV with remote data
"""
import math

from pyairtable import Api
import pandas as pd
import yaml

from tqdm import tqdm

# Airtable credentials and base information
with open('config.yaml', 'r', encoding='utf-8') as config_file:
    CONFIG = yaml.safe_load(config_file)
API_KEY = CONFIG['airtable']['api-key']
BASE_ID = CONFIG['airtable']['base-id']
TABLE_ID = CONFIG['airtable']['table-id']

# Initialize Airtable client
api = Api(API_KEY)
table = api.table(BASE_ID, TABLE_ID)

def convert_duration(duration_string):
    """Converts ISO 8601 duration to seconds"""
    duration = pd.Timedelta(duration_string).total_seconds()
    return duration

def classify_video(row, special):
    """Classifies video into special, guest or regular episode"""
    if special:
        video_type = "Special"
    elif "ft. " in row.Title:
        video_type = "Guest"
    else:
        video_type = "Regular"
    return video_type

def get_video_id_from_url(url):
    return url.split('watch?v=')[-1]

def update_video_stats_in_airtable():
    from youtube import get_youtube_video_stats
    existing_records = table.all(fields=['URL', 'Views', 'Likes', 'Comments'])
    for record in tqdm(existing_records, desc="Updating existing records"):
        url = record['fields']['URL']
        views, likes, comments = get_youtube_video_stats(get_video_id_from_url(url))     
        if views is not None and likes is not None and comments is not None:
            table.update(record['id'], {
                'Views': views,
                'Likes': likes,
                'Comments': comments
            })
        else:
            print(f"Could not retrieve stats for URL {url}")
    else:
        print("Updated all existing records in Airtable")

def push_csv_data_to_airtable(csv_file, special):
    """Pushes data to Airtable from each CSV"""
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Get existing record URLs from Airtable to check against
    existing_records = table.all(fields=['URL'])
    existing_urls = [record['fields']['URL'] for record in existing_records]
    update_video_stats_in_airtable()

    # Iterate through DataFrame rows and push data to Airtable if URL doesn't exist
    for row in tqdm(df.itertuples(index=False), desc="Inserting new records"):
        if row.URL not in existing_urls:
            record_data = {
                'URL': row.URL,
                'Number': None if math.isnan(row.Number) else row.Number,
                'Title': row.Title,
                'Duration': convert_duration(row.Duration),
                'Date': row.Date,
                'Views': row.Views,
                'Likes': row.Likes,
                'Comments': row.Comments,
                'Special/Guest': classify_video(row, special),
            }
            # Insert record into Airtable
            table.create(record_data)
        else:
            print(f"Record with URL {row.URL} already exists in Airtable. Skipping insertion.")

def main():
    """Main function"""
    # For specials
    push_csv_data_to_airtable('./data/specials.csv', special=True)
    print('Pushed specials to Airtable.')

    # For regular videos
    push_csv_data_to_airtable('./data/episodes.csv', special=False)
    print('Pushed regular videos to Airtable.')

if __name__ == "__main__":
    main()
