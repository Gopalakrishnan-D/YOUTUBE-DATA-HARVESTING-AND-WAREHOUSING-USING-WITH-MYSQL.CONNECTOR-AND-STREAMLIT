# Importing Libraries of execution 
import googleapiclient.discovery
from googleapiclient.discovery import build
import mysql.connector as sql
import isodate
from datetime import datetime
from streamlit_option_menu import option_menu
import streamlit as st
from googleapiclient.errors import HttpError
import pandas as pd

# API CONNECTION

def Api_connect():
    Api_Key = "AIzaSyAD736C1J8WISbH0M4i5rYlV1iovFhCB7k"  # Replace with your actual API key
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Key)
    return youtube

youtube = Api_connect()

# FETCHING THE CHANNEL ID:
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    data = {
        "Channel_Name": response['items'][0]['snippet']['title'],
        "Channel_Id": channel_id,
        "Channel_Description": response['items'][0]['snippet']['description'],
        "Channel_Playlist": response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "Channel_Videocount":response['items'][0]['statistics']['videoCount'],
        "Channel_Subscription": response['items'][0]['statistics']['subscriberCount']
    }
    return data

# FETCHING THE VIDEO ID:
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        response1 = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids
    
# FETCHING THE VIDEO INFORMATION:
def duration_to_seconds(video_duration):
    duration = isodate.parse_duration(video_duration)
    hours = duration.days * 24 + duration.seconds // 3600
    minutes = (duration.seconds % 3600) // 60
    seconds = duration.seconds % 60
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def published_date(video_publisheddate):
    iso_date = datetime.fromisoformat(video_publisheddate.replace("Z", "+00:00"))
    sql_date_str = iso_date.strftime('%Y-%m-%d %H:%M:%S')
    return sql_date_str

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item.get('tags'),
                        Description=item.get('description'),
                        video_Publisheddate=published_date(item['snippet'].get('publishedAt')),
                        video_duration=duration_to_seconds(item['contentDetails'].get('duration')),
                        Views=item['statistics'].get('viewCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_status=item['contentDetails']['caption'],  
                    ) 
            video_data.append(data)
    return video_data

# FETCHING THE COMMENT INFORMATION:
def comment_date(comment_published):
    iso_date = datetime.fromisoformat(comment_published.replace("Z", "+00:00"))
    sql_date_str = iso_date.strftime('%Y-%m-%d %H:%M:%S')
    return sql_date_str

def get_comment_info(video_ids):
    Comment_data = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            if 'items' in response:
                for item in response['items']:
                    data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                                Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_Published=comment_date(item['snippet']['topLevelComment']['snippet'].get('publishedAt'))
                    )
                    Comment_data.append(data)
            else:
                st.error(f"Comments are disabled for video {video_id}")
        except HttpError as e:
            if e.resp.status == 403:
                st.error(f"Comments are disabled for video {video_id}")
            else:
                st.error(f"An HTTP error occurred: {e}")
    return Comment_data
    
# FETCHING THE PLAYLIST DETAILS:
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails', 
            channelId=channel_id,                      
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
    
        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

channel_details=get_channel_info("UCEOtpkAT-Z6Y_4Nh8BP1Cyg")
#channel_details

Video_Ids=get_videos_ids("UCEOtpkAT-Z6Y_4Nh8BP1Cyg")
#Video_Ids

video_details=get_video_info(Video_Ids)
#video_details

comment_details=get_comment_info(Video_Ids)
#comment_details
    
# SQL CONNECTION:
# Connect to MySQL server
DB = sql.connect(host="localhost",user="root",password="root")
mycursor = DB.cursor()

# CREATE DATABASE:
mycursor.execute("CREATE DATABASE IF NOT EXISTS world")

#CREATE CHANNEL TABLE AND INSERT DATA :
DB = sql.connect(host="localhost",user="root",password="root",database="world")
mycursor = DB.cursor()
mycursor.execute("""CREATE TABLE IF NOT EXISTS Channels(
                        Channel_Name VARCHAR(255),
                        Channel_Id VARCHAR(255) PRIMARY KEY,
                        Channel_Description TEXT, 
                        Channel_Playlist VARCHAR(255), 
                        Channel_Videocount INT,
                        Channel_Subscription INT)
                    """)
DB.commit()

DB = sql.connect(host="localhost",user="root",password="root",database="world")
mycursor = DB.cursor()

query = """INSERT INTO channels (Channel_Name, Channel_Id, Channel_Description, Channel_Playlist, Channel_Videocount, Channel_Subscription) VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE Channel_Name=VALUES(Channel_Name), Channel_Description=VALUES(Channel_Description),Channel_Playlist =VALUES(Channel_Playlist),Channel_Videocount=VALUES(Channel_Videocount),Channel_Subscription=VALUES(Channel_Subscription)"""
values = tuple(channel_details.values())
mycursor.execute(query, values)
DB.commit()

# Create VIDEO TABLE
mycursor.execute("""CREATE TABLE IF NOT EXISTS VIDEOS(
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Video_Id VARCHAR(100) PRIMARY KEY,
                        Title VARCHAR(100),
                        Tags VARCHAR(255),
                        Description TEXT, 
                        video_Publisheddate VARCHAR(100), 
                        Video_duration INT,
                        Views INT,
                        Comments INT, 
                        Favorite_Count INT, 
                        Definition VARCHAR(100),
                        Caption_status VARCHAR(100))
                    """)
DB.commit()

video_query = "INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Description, video_Publisheddate, Video_duration, Views, Comments, Favorite_Count, Definition, Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Channel_Name=VALUES(Channel_Name), Channel_Id=VALUES(Channel_Id), Title=VALUES(Title), Tags=VALUES(Tags), Description=VALUES(Description), video_Publisheddate=VALUES(video_Publisheddate), Video_duration=VALUES(Video_duration), Views=VALUES(Views), Comments=VALUES(Comments), Favorite_Count=VALUES(Favorite_Count), Definition=VALUES(Definition), Caption_Status=VALUES(Caption_Status)"
temp = [tuple(item.values()) for item in video_details]
mycursor.executemany(video_query, temp)
DB.commit()

mycursor.execute("""
    CREATE TABLE IF NOT EXISTS Comments(
        Comment_ID VARCHAR(255) PRIMARY KEY,
        Video_ID VARCHAR(255),
        Comment_Text TEXT,
        Comment_Author VARCHAR(255),
        Comment_Published VARCHAR(255)
        )
    """)
DB.commit()

Comment_query = "INSERT INTO comments (Comment_ID, Video_ID, Comment_Text, Comment_Author, Comment_Published) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Video_ID=VALUES(Video_ID), Comment_Text=VALUES(Comment_Text), Comment_Author=VALUES(Comment_Author), Comment_Published=VALUES(Comment_Published)"
com = [tuple(item.values()) for item in comment_details]
mycursor.executemany(Comment_query, com)
DB.commit()


# SETUP STREAMLIT UI:
st.set_page_config(page_title="GUVI's PROJECT -YouTube Data Harvesting and Warehousing", layout="wide")

DB = sql.connect(host="localhost",user="root",password="root",database="world")
mycursor = DB.cursor()

def run_query(query):
    mycursor.execute(query)
    result = mycursor.fetchall()
    columns = [i[0] for i in mycursor.description]
    return pd.DataFrame(result, columns=columns)

# SETUP SIDEBAR:
with st.sidebar:
    st.header("NAVIGATION")
    section = st.selectbox("SELECT SECTION", ["HOME", "DATA COLLECTION", "DATA ANALYSIS"])

# HOME SECTION:# TITLE & DESCRIPTION:


if section == "HOME":
    st.title(':rainbow[YOUTUBE DATA HARVESTING and WAREHOUSING USING MYSQL AND STREAMLIT]')
    st.markdown("## :red[DOMAIN] : Social Media")
    st.markdown("## :red[SKILLS] : Python scripting, API connection, Data collection, Table Creation, Data Insertion, Streamlit")
    st.markdown("## :red[OVERALL VIEW] : Creating an UI with Streamlit, retrieving data from YouTube API, storing the data in SQL as a WH, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")
    st.markdown("## :red[DEVELOPED BY] : GOPALA KRISHNAN D")
    st.header("Welcome to the YouTube Data Harvesting and Warehousing App!")
    st.markdown("Use the sidebar to navigate through different sections of the app.")
    
    # DATA COLLECTION SECTION:
def main(): 
    if section == "DATA COLLECTION":
        st.header("DATA COLLECTION")
        st.markdown("Enter the YouTube Channel ID to collect and store data.")
    
        channel_id = st.text_input("Enter YouTube Channel ID:")
        if st.button("Collect and Store Data"):
            st.success(f"Data for channel ID collected and stored successfully.")
            channel_details = get_channel_info(channel_id)
            Video_Ids = get_videos_ids(channel_id)
            video_details = get_video_info(Video_Ids)
            comment_details = get_comment_info(Video_Ids)
            
            ch_query = "INSERT IGNORE INTO channels (Channel_Name, Channel_Id, Channel_Description, Channel_Playlist, Channel_Videocount, Channel_Subscription) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Channel_Name=VALUES(Channel_Name), Channel_Description=VALUES(Channel_Description),Channel_Playlist =VALUES(Channel_Playlist),Channel_Videocount=VALUES(Channel_Videocount),Channel_Subscription=VALUES(Channel_Subscription)"
            values = tuple(channel_details.values())
            mycursor.execute(ch_query, values)
            DB.commit()
            
            vid_query = "INSERT IGNORE INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Description, video_Publisheddate, Video_duration, Views, Comments, Favorite_Count, Definition, Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Channel_Name=VALUES(Channel_Name), Channel_Id=VALUES(Channel_Id), Title=VALUES(Title), Tags=VALUES(Tags), Description=VALUES(Description), video_Publisheddate=VALUES(video_Publisheddate), Video_duration=VALUES(Video_duration), Views=VALUES(Views), Comments=VALUES(Comments), Favorite_Count=VALUES(Favorite_Count), Definition=VALUES(Definition), Caption_Status=VALUES(Caption_Status)"
            temp = [tuple(item.values()) for item in video_details]
            mycursor.executemany(vid_query, temp)
            DB.commit()
            
            com_query = "INSERT IGNORE INTO comments (Comment_ID, Video_ID, Comment_Text, Comment_Author, Comment_Published) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Video_ID=VALUES(Video_ID), Comment_Text=VALUES(Comment_Text), Comment_Author=VALUES(Comment_Author), Comment_Published=VALUES(Comment_Published)"
            com = [tuple(item.values()) for item in comment_details]
            mycursor.executemany(com_query, com)
            DB.commit()

if __name__=="__main__":
    main()                

# DATA ANALYSIS SECTION:

if section == "DATA ANALYSIS":
    st.header("DATA ANALYSIS")

    questions = [
        "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?",
    ]
    query = ""   
    selected_question = st.selectbox("Select a Question to Query", questions)
    # Function to execute SQL queries and return the results as a DataFrame

    
    if st.button("Run Query"):
        if selected_question == questions[0]:
            query = "SELECT Title AS Video_Name, Channel_Name FROM Videos"
        elif selected_question == questions[1]:
            query = "SELECT Channel_Name, COUNT(Video_Id) AS Number_of_Videos FROM videos GROUP BY Channel_Name ORDER BY Number_of_Videos DESC"
        elif selected_question == questions[2]:
            query = "SELECT Title AS Video_Name, Channel_Name, Views FROM videos ORDER BY Views DESC LIMIT 10"
        elif selected_question == questions[3]:
            query = "SELECT Title AS Video_Name, Comments FROM videos"
        elif selected_question == questions[4]:
            query = "SELECT Title AS Video_Name, Channel_Name, Favorite_Count FROM videos ORDER BY Favorite_Count DESC LIMIT 10"  # Use Favorite_Count instead of Likes
        elif selected_question == questions[5]:
            query = "SELECT Channel_Name, SUM(Views) AS Total_Views FROM videos GROUP BY Channel_Name"
        elif selected_question == questions[6]:
            query = "SELECT Channel_Name FROM videos WHERE video_Publisheddate LIKE '2022%' GROUP BY Channel_Name"
        elif selected_question == questions[7]:
            query = "SELECT Channel_Name, AVG(Video_duration) AS Average_Duration FROM videos GROUP BY Channel_Name"
        elif selected_question == questions[8]:
            query = "SELECT Title AS Video_Name, Channel_Name, Comments FROM videos ORDER BY Comments DESC LIMIT 10"
        else:
            query = ""

        if query:
            result_df = run_query(query)
            st.write(result_df)
        else:
            st.error("Invalid query selected. Please try again.")