from googleapiclient.discovery import build
import pandas as pd
import re
from datetime import datetime
import yaml
import airtable

with open('config.yaml', 'r', encoding='utf-8') as config_file:
    CONFIG = yaml.safe_load(config_file)
API_KEY = CONFIG['youtube']['api-key']
CHANNEL_ID = CONFIG['youtube']['channel-id']

# Build the YouTube API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Function to get the last processed date
def get_last_processed_date():
    try:
        with open('last_processed.txt', 'r') as file:
            last_processed_date = file.readline().strip()
            return datetime.strptime(last_processed_date, '%Y-%m-%dT%H:%M:%SZ')
    except FileNotFoundError:
        return None

# Function to update the last processed date
def update_last_processed_date(date):
    with open('last_processed.txt', 'w') as file:
        file.write(date)

# Function to get recent video IDs from a channel
def get_recent_video_ids(channel_id, published_after):
    video_ids = []
    request = youtube.search().list(
        part='id,snippet',
        channelId=channel_id,
        maxResults=50,
        order='date',
        publishedAfter=published_after.isoformat() + 'Z',
        type='video'
    )
    
    response = request.execute()
    while response:
        for item in response['items']:
            published_at = item['snippet']['publishedAt']
            if item['id']['kind'] == 'youtube#video' and published_at > published_after.isoformat() + 'Z':
                video_ids.append({
                    'videoId': item['id']['videoId'],
                    'publishedAt': published_at
                })
        
        if 'nextPageToken' in response:
            request = youtube.search().list(
                part='id,snippet',
                channelId=channel_id,
                maxResults=50,
                order='date',
                pageToken=response['nextPageToken'],
                publishedAfter=published_after.isoformat() + 'Z',
                type='video'
            )
            response = request.execute()
        else:
            break
    return video_ids

# Function to get video details by video ID
def get_video_details(video_ids):
    videos = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join([video['videoId'] for video in video_ids[i:i+50]])
        )
        response = request.execute()
        for item in response['items']:
            category, episode_number = categorize_video(item['snippet']['title'])
            title = clean_title(item['snippet']['title'])
            video_data = {
                'Title': title,
                'Number': episode_number,
                'URL': f"https://www.youtube.com/watch?v={item['id']}",
                'Duration': item['contentDetails']['duration'],
                'Date': item['snippet']['publishedAt'],
                'Views': int(item['statistics'].get('viewCount', 0)),
                'Likes': int(item['statistics'].get('likeCount', 0)),
                'Comments': int(item['statistics'].get('commentCount', 0)),
                'Category': category,
            }
            videos.append(video_data)
    return videos

# Function to categorize video based on title
def categorize_video(title):
    if "Trash Taste Special" in title:
        return "Special", None
    elif "#shorts" in title:
        return "Short", None
    elif re.search(r'#\d+$', title):
        episode_number = re.search(r'#(\d+)$', title).group(1)
        return "Episode", episode_number
    else:
        return "Uncategorized", None

# Remove the last 4 words of title
def clean_title(title):
    return ' '.join(title.split()[:-4])

# Function to append new data to the top of a CSV file
def append_to_csv(file_name, new_data):
    try:
        existing_data = pd.read_csv(file_name)
        updated_data = pd.concat([pd.DataFrame(new_data), existing_data], ignore_index=True)
    except FileNotFoundError:
        updated_data = pd.DataFrame(new_data)
    updated_data.to_csv(file_name, index=False)

# Main script logic
def main():
    last_processed_date = get_last_processed_date()
    if not last_processed_date:
        last_processed_date = datetime(2000, 1, 1)  # Default to an old date if not found

    # Get recent video IDs
    recent_videos = get_recent_video_ids(CHANNEL_ID, last_processed_date)

    if not recent_videos:
        print("No new videos found.")
        return

    # Get details for the recent videos
    video_details = get_video_details(recent_videos)

    # Update the last processed date to the most recent video
    most_recent_date = max(video['Date'] for video in video_details)
    update_last_processed_date(most_recent_date)

    # Separate video details by category and append to corresponding CSV files
    specials = [video for video in video_details if video['Category'] == 'Special']
    shorts = [video for video in video_details if video['Category'] == 'Short']
    episodes = [video for video in video_details if video['Category'] == 'Episode']

    if specials:
        append_to_csv('specials.csv', specials)
    if shorts:
        append_to_csv('shorts.csv', shorts)
    if episodes:
        append_to_csv('episodes.csv', episodes)

    print("New video data appended to CSV files.")

    airtable.main()  # Push new data to Airtable

if __name__ == "__main__":
    main()
