# Trash Taste Data Project
I am a longtime follower of the [Trash Taste Podcast](https://www.youtube.com/@TrashTaste). So, to learn more about things like Airtable, Pandas, the YouTube API and manipulating Data in Python, I took it upon myself to find a way to compile statistics related to my favourite podcast's videos.

## Explanation (an attempt)
Let's start from `youtube.py`. This file is entirely responsible for communicating with the YouTube API. Similarly, `airtable.py` is responsible only for communicating with the Airtable API.

The API key I set up in the Google Cloud Console is taken from the `config.yaml` file. The script then checks for a "last processed date" -- that is, the date of the last video that was fetched by the script. It then searches only for Trash Taste Podcast videos that were uploaded after that date. If new videos are found, their data is looked up through the API (statistics such as views, likes, comments). The video is then classified as a 'special', a 'short', or an 'episode' (based on the title -- thankfully, the podcast uses a very strict and consistent naming scheme, so this was not hard). The new videos are then appended to their own .csv documents based on their classification.

The Airtable script then goes through all three .csv documents to see if there are any new videos of that type. If there are any such videos, they are uploaded to the table. The script also goes through existing records in the table (that is, the Airtable) to update the amount of views, likes and comments each has.

I plan to set this up on a Raspberry Pi.