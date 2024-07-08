from pyairtable import Api
import pandas as pd
from datetime import datetime, timedelta

# Airtable credentials and base information
AIRTABLE_API_KEY = 'patJd3GqBnH8iBR0f.e36c74c76da7eb0947f599aae7d1c8cf49321a93b0ec9b6964d5aa724168b3ba'
AIRTABLE_BASE_ID = 'apppeWfhzZdOuvFH6'
AIRTABLE_TABLE_ID = 'tblqDQ5oK8Je8Dfpu'

# Initialize Airtable client
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

# Function to convert ISO 8601 duration to h:mm:ss format
def convert_duration(duration_string):
    duration = pd.Timedelta(duration_string).to_pytimedelta()
    
    # Format duration as h:mm:ss
    hours = duration.seconds // 3600
    minutes = (duration.seconds // 60) % 60
    seconds = duration.seconds % 60
    
    return f"{hours:02}:{minutes:02}:{seconds:02}"

# Function to read CSV file and push data to Airtable
# Function to push new data from CSV to Airtable
def push_csv_data_to_airtable(csv_file):
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Get existing record URLs from Airtable to check against
    existing_records = table.all(fields=['URL'])
    existing_urls = [record['fields']['URL'] for record in existing_records]

    # Iterate through DataFrame rows and push data to Airtable if URL doesn't exist
    for index, row in df.iterrows():
        if row['URL'] not in existing_urls:
            record_data = {
                'URL': row['URL'],
                'Title': row['Title'],
                'Duration': convert_duration(row['Duration']),
                'Date': row['Date'],
                'Views': row['Views'],
                'Likes': row['Likes'],
                'Number of Comments': row['Number of Comments']
                # Add more fields as needed based on your Airtable schema
            }
            # Insert record into Airtable
            table.create(record_data)
        else:
            print(f"Record with URL {row['URL']} already exists in Airtable. Skipping insertion.")

# Push data from each CSV file to Airtable
def main():
    csv_files = ['specials.csv', 'episodes.csv']  # List of CSV files to process
    
    for csv_file in csv_files:
        push_csv_data_to_airtable(csv_file)
        print(f"Data from {csv_file} pushed to Airtable.")

if __name__ == "__main__":
    main()