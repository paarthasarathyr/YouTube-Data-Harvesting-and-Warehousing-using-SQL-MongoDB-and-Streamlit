# Importing YouTube API, MongoDB, MySQL, and Streamlit 

from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st
import mysql.connector



# Establishing Connection With API Key

def api_connect():
    api_id = 'AIzaSyAqxT8f-HYwqKxuOZBrTyAo46JBf1kaKoY'
    api_service_name = 'youtube'
    api_version = 'v3'

    youtube = build(api_service_name, api_version, developerKey = api_id)

    return youtube

youtube = api_connect()



# Fetching Channel Information From YouTube API

request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = "UCT8FGLq9AU1H6U3ePhegxJA"
)

response = request.execute()



# Function To Fetch Youtube Channel Information

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id
    )
    response = request.execute()  

    for item in response['items']:
        channel_data = dict(channel_name = item['snippet']['title'],
                            channel_id = item['id'],
                            channel_description = item['snippet']['description'],
                            channel_playlist = item['contentDetails']['relatedPlaylists']['uploads'],
                            channel_viewcount = item['statistics']['viewCount'],
                            channel_subcribers = item['statistics']['subscriberCount'],
                            channel_videocount = item['statistics']['videoCount']
        )
    return channel_data 



# Function To Fetch All Video IDs Of The Channel

def get_videos_ids(channel_id):
    video_ids = []

    response = youtube.channels().list(part='contentDetails', id=channel_id).execute()

    # Retrieving YouTube Channel's Uploads Playlist

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        # Fetching All Videos From Uploads Playlist

        playlist_items = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,  
            pageToken=next_page_token
        ).execute()

        # Append video IDs To The List
        
        for item in playlist_items['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token = playlist_items.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

Video_Ids = get_videos_ids('UCT8FGLq9AU1H6U3ePhegxJA')



# To Collect All Video Details

for video_id in Video_Ids:
    request = youtube.videos().list(
        part = "snippet,contentDetails,statistics",
        id = video_id
    )
    response = request.execute()



# Function To Fetch All Video Information Of The Channel

def get_video_info(Video_Ids):
    v_data = []
    for video_id in Video_Ids:
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id = video_id
        )
        response = request.execute()

        ## all info

        for item in response["items"]:
            video_data = dict(Channel_Name = item['snippet']['channelTitle'],
                            Channel_ID = item['snippet']['channelId'],
                            Video_Title = item['snippet']['title'],
                            Video_ID = item['id'],
                            Thumbnail = item['snippet']['thumbnails']['default']['url'],
                            Description = item['snippet'].get('description'),
                            Published_Date = item['snippet']['publishedAt'],
                            Tags = item['snippet'].get('tags'),
                            Duration = item['contentDetails']['duration'],
                            Defnination = item['contentDetails']['definition'],
                            Views = item['statistics'].get('viewCount'),
                            Likes = item['statistics'].get('likeCount'),
                            Comment_count = item['statistics'].get('commentCount'),
                            Favourite_count = item['statistics']['favoriteCount'],
                            Like_count = item['statistics'].get('likeCount'),
                            )
            v_data.append(video_data)
    return v_data



# Function To Fetch All Comments Information Of The Channel


def get_comment_info(Video_Ids):
    c_data = []
    try:
        for video_id in Video_Ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()
            for item in response["items"]:
                comment_data = dict(comment_Id = item["snippet"]["topLevelComment"]["id"],
                        Video_Id = item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Commet_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_publication = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                
                c_data.append(comment_data)
    except:
        pass 
        return c_data 



# Function To Fetch All Playlists Information Of The Channel

def get_playlist_info(channel_id):
    next_page_token = None
    p_data = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            playlist_data = dict(Playlist_Id = item['id'],
                                Channel_Id = item['snippet']['channelId'],
                                Title = item['snippet']['title'],
                                Channel_Name = item['snippet']['channelTitle'],
                                PublishedAt = item['snippet']['publishedAt'],
                                Video_Count = item['contentDetails']['itemCount'])
            p_data.append(playlist_data)

        next_page_token = response.get('nextPageToken')   
        if next_page_token is None:
            break
    return p_data 



# Connecting To MongoDB For YouTube Data Storage

client = pymongo.MongoClient("mongodb+srv://paarthasarathyrengasamy:sarathyr@cluster0.nijfyq8.mongodb.net/?retryWrites=true&w=majority")

db = client["youtube_collection_data"]



# Complete YouTube Data Insertion

def channel_details(channel_id):
    channel_info = get_channel_info(channel_id)
    playlist_info = get_playlist_info(channel_id)
    video_ids = get_videos_ids(channel_id) 
    video_info = get_video_info(video_ids)
    comment_info = get_comment_info(video_ids)

    data_collection = db['channel_details']
    data_collection.insert_one({"channel_information" : channel_info, "playlist_information" : playlist_info,
                      "video_information" : video_info, "comment_information" : comment_info })
    
    return "upload completed successfully"



# All Channel Information

channel_list = []
db = client["youtube_collection_data"]
data_collection = db['channel_details']
for ch_data in data_collection.find({}, {"_id": 0, "channel_information": 1}):  # Correct the field name
    print(ch_data["channel_information"])



channel_list = []
db = client["youtube_collection_data"]
data_collection = db['channel_details']
for ch_data in data_collection.find({}, {"_id": 0, "channel_information": 1}):  # Correct the field name
    channel_list.append(ch_data["channel_information"])



# Creating DataFrame for YouTube Channel Data

channel_list = []
db = client["youtube_collection_data"]
data_collection = db['channel_details']
for ch_data in data_collection.find({}, {"_id": 0, "channel_information": 1}):  # Correct the field name
    channel_list.append(ch_data["channel_information"])

df = pd.DataFrame(channel_list)



# Creating Channel Information Table

# Establish a connection to the MySQL database

def channels_table():

    mydb = mysql.connector.connect(
            host="localhost",
            user="paart",
            password="2352",
            database="data_collection_from_youtube",
            port="3306"
        )


    # Creating a cursor to execute SQL queries
    
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    # Creating a table for channels

    try:
        create_query ='''create table if not exists channels(channel_name varchar(100),
                                                                channel_id varchar(100) primary key,
                                                                channel_description text,
                                                                channel_playlist varchar(100),
                                                                channel_viewcount bigint,
                                                                channel_subcribers bigint,
                                                                channel_videocount bigint)'''
        
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")    

    channel_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for ch_data in data_collection.find({}, {"_id": 0, "channel_information": 1}):  
        channel_list.append(ch_data["channel_information"])

    df = pd.DataFrame(channel_list)  


    for index,row in df.iterrows():
        insert_query = '''insert into channels( channel_name,
                                                channel_id,
                                                channel_description,
                                                channel_playlist,
                                                channel_viewcount,
                                                channel_subcribers,
                                                channel_videocount)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
            row['channel_id'],
            row['channel_description'],
            row['channel_playlist'],
            row['channel_viewcount'],
            row['channel_subcribers'],
            row['channel_videocount'])

        try:
            cursor.execute(insert_query,values)     
            mydb.commit()
        except:
            print("channels values are already inserted")



# Building Playlist Information Table

playlist_list = []
db = client["youtube_collection_data"]
data_collection = db['channel_details']
for pl_data in data_collection.find({}, {"_id": 0, "playlist_information": 1}): 
    for i in range(len(pl_data["playlist_information"])):
        playlist_list.append(pl_data["playlist_information"][i])

df1 = pd.DataFrame(playlist_list) 



# Creating Playlists Information Function


def playlist_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="paart",
        password="2352",
        database="data_collection_from_youtube",
        port="3306"
    )

    # Create a cursor to execute SQL queries
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS playlists(
                        Playlist_Id VARCHAR(100) PRIMARY KEY,
                        Channel_Id VARCHAR(100),
                        Title VARCHAR(100),
                        Channel_Name VARCHAR(100),
                        PublishedAt TIMESTAMP,
                        Video_Count INT
                    )'''
    cursor.execute(create_query)
    mydb.commit()    

    playlist_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for pl_data in data_collection.find({}, {"_id": 0, "playlist_information": 1}): 
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])

    df1 = pd.DataFrame(playlist_list)

    from datetime import datetime

    # ...

    for index, row in df1.iterrows():
        # Convert ISO 8601 datetime string to MySQL-compatible format
        published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        insert_query = '''insert into playlists(Playlist_Id,
                                                Channel_Id,
                                                Title,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                        values(%s, %s, %s, %s, %s, %s)'''

        values = (row['Playlist_Id'],
                row['Channel_Id'],
                row['Title'],
                row['Channel_Name'],
                published_at,  # Use the formatted datetime string
                row['Video_Count'])

        cursor.execute(insert_query, values)
        mydb.commit()



# Creating Video Information Table

video_list = []
db = client["youtube_collection_data"]
data_collection = db['channel_details']
for vi_data in data_collection.find({}, {"_id": 0, "video_information": 1}): 
    for i in range(len(vi_data["video_information"])):
        video_list.append(vi_data["video_information"][i])

df2 = pd.DataFrame(video_list)



# Creating a Function for Table Videos

def videos_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="paart",
        password="2352",
        database="data_collection_from_youtube",
        port="3306"
    )

    # Create a cursor to execute SQL queries

    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS videos(
                        Channel_Name VARCHAR(100),
                        Channel_ID VARCHAR(100),
                        Video_Title VARCHAR(100),
                        Video_ID VARCHAR(100) PRIMARY KEY,
                        Thumbnail VARCHAR(200),
                        Description TEXT,
                        Published_Date TIMESTAMP,
                        Tags TEXT,
                        Duration BIGINT,
                        Defnination VARCHAR(50),
                        Views BIGINT,
                        Likes BIGINT,
                        Comment_count INT,
                        Favourite_count INT,
                        Like_count INT
                        )'''
    cursor.execute(create_query)
    mydb.commit()  

    video_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for vi_data in data_collection.find({}, {"_id": 0, "video_information": 1}): 
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data["video_information"][i])

    df2 = pd.DataFrame(video_list)


    from datetime import datetime

    # ...

    for index, row in df2.iterrows():
        # Convert ISO 8601 datetime string to MySQL-compatible format

        published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        # Convert the list to a comma-separated string

        tags_str = ', '.join(map(str, row['Tags'])) if row['Tags'] is not None else ''



        # Convert ISO 8601 duration string to seconds
        
        iso_duration = row['Duration']
        duration_seconds = 0

        if 'T' in iso_duration:
            time_part = iso_duration.split('T')[1]

            if 'H' in time_part:
                hours, time_part = time_part.split('H')
                duration_seconds += int(hours) * 3600

            if 'M' in time_part:
                minutes = int(time_part.split('M')[0])
                duration_seconds += minutes * 60

                if 'S' in time_part:
                    seconds = int(time_part.split('M')[1].split('S')[0])
                    duration_seconds += seconds
            elif 'S' in time_part:
                duration_seconds += int(time_part.split('S')[0])


        elif 'S' in iso_duration:
            duration_seconds += int(iso_duration.split('S')[0])

        insert_query = '''INSERT INTO videos(Channel_Name, 
                                            Channel_ID, 
                                            Video_Title, 
                                            Video_ID,
                                            Thumbnail, 
                                            Description, 
                                            Published_Date, 
                                            Tags, 
                                            Duration, 
                                            Defnination, 
                                            Views, 
                                            Likes, 
                                            Comment_count, 
                                            Favourite_count, 
                                            Like_count)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                        ON DUPLICATE KEY UPDATE
                        Channel_Name=VALUES(Channel_Name), 
                        Channel_ID=VALUES(Channel_ID), 
                        Video_Title=VALUES(Video_Title), 
                        Thumbnail=VALUES(Thumbnail),
                        Description=VALUES(Description), 
                        Published_Date=VALUES(Published_Date), 
                        Tags=VALUES(Tags), 
                        Duration=VALUES(Duration),
                        Defnination=VALUES(Defnination), 
                        Views=VALUES(Views), 
                        Likes=VALUES(Likes), 
                        Comment_count=VALUES(Comment_count),
                        Favourite_count=VALUES(Favourite_count), 
                        Like_count=VALUES(Like_count)'''

        values = (row['Channel_Name'], 
                row['Channel_ID'], 
                row['Video_Title'], 
                row['Video_ID'], 
                row['Thumbnail'], 
                row['Description'],
                published_date, 
                tags_str, 
                duration_seconds, 
                row['Defnination'], 
                row['Views'], 
                row['Likes'],
                row['Comment_count'], 
                row['Favourite_count'], 
                row['Like_count']
                )



        cursor.execute(insert_query, values)
        mydb.commit()



# Creating DataFrame for Comments Table

comment_list = []
db = client["youtube_collection_data"]
data_collection = db['channel_details']
for com_data in data_collection.find({}, {"_id": 0, "comment_information": 1}): 
    for i in range(len(com_data["comment_information"])):
        comment_list.append(com_data["comment_information"][i])

df3 = pd.DataFrame(comment_list)



## Creating Comment Table in MySQL

def comments_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="paart",
        password="2352",
        database="data_collection_from_youtube",
        port="3306"
    )

    # Create a cursor to execute SQL queries

    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''CREATE TABLE IF NOT EXISTS comments(
                        comment_Id VARCHAR(100) PRIMARY KEy,
                        Video_Id VARCHAR(100),
                        Comment_Text TEXT,
                        Commet_Author VARCHAR(100),
                        Comment_publication TIMESTAMP
                        )'''
    cursor.execute(create_query)
    mydb.commit() 

    comment_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for com_data in data_collection.find({}, {"_id": 0, "comment_information": 1}): 
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])

    df3 = pd.DataFrame(comment_list)

    from datetime import datetime
    # ...

    for index, row in df3.iterrows():
        # Convert ISO 8601 datetime string to MySQL-compatible format
        
        comment_publ = datetime.strptime(row['Comment_publication'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        insert_query = '''insert into comments(comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Commet_Author,
                                                Comment_publication
                                                )
                            values(%s, %s, %s, %s, %s)'''

        values = (row['comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Commet_Author'],  # Use the formatted datetime string
                comment_publ
                )

        cursor.execute(insert_query, values)
        mydb.commit()



# Creating Database Tables for Channel, Video, Playlist, and Comment Information

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"



Tables = tables()



# Initializing Streamlit for YouTube Data Exploration

def show_channels_table():
    channel_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for ch_data in data_collection.find({}, {"_id": 0, "channel_information": 1}):  # Correct the field name
        channel_list.append(ch_data["channel_information"])

    df = st.dataframe(channel_list)

    return df



def show_playlist_table():
    playlist_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for pl_data in data_collection.find({}, {"_id": 0, "playlist_information": 1}): 
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])

    df1 = st.dataframe(playlist_list) 

    return df1



def show_videos_table():
    video_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for vi_data in data_collection.find({}, {"_id": 0, "video_information": 1}): 
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data["video_information"][i])

    df2 = st.dataframe(video_list)

    return df2



def show_comments_table():
    comment_list = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for com_data in data_collection.find({}, {"_id": 0, "comment_information": 1}): 
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])

    df3 = st.dataframe(comment_list)

    return df3



# Designing the User Interface for YouTube Data Exploration

# Set page title and icon

st.set_page_config(
    page_title="YouTube Data Exploration",
    page_icon=":red_circle:"
)

# Sidebar

with st.sidebar:
    st.title(":violet[YouTube Data Dive: Script, Collect, and Analyze]")
    st.markdown("## Description")
    st.write("- Explore the vast realm of YouTube data with Python scripting, efficient data collection, seamless API integration, and robust data management using MongoDB and SQL.")
    st.write("- Effortlessly explore and visualize YouTube data, transforming raw information into actionable insights.")
    

# Main content
    
st.title(":red[Explore YouTube Data: Harvesting and Warehousing]")
st.header("Welcome to the YouTube Data Exploration Dashboard!")

# Interactive Channel ID Input for YouTube Data Exploration

channel_id = st.text_input("Enter the Channel ID")

# Data Collection and Storage based on Channel ID in MongoDB

if st.button("Collect and store data"):
    channel_ids = []
    db = client["youtube_collection_data"]
    data_collection = db['channel_details']
    for ch_data in data_collection.find({}, {"_id": 0, "channel_information": 1}):  # Correct the field name
        channel_ids.append(ch_data["channel_information"]['channel_id'])

        if channel_id in channel_ids:
            st.success("Channel Details of the given Channel ID already exists")

        else:
            insert = channel_details(channel_id)    
            st.success(insert)

# Migrate Data to SQL Database
            
if st.button("Migrate to SQL"):
    table = tables()
    st.success(table)

# Select Table for Viewing
    
show_table = st.radio("Select the Table fro View",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS")) 
if show_table == "CHANNELS":
    show_channels_table()

elif show_table == "PLAYLISTS":
    show_playlist_table()

elif show_table == "VIDEOS":
    show_videos_table()

elif show_table == "COMMENTS":
    show_comments_table()                           
    


# SQL Connection

mydb = mysql.connector.connect(
    host="localhost",
    user="paart",
    password="2352",
    database="data_collection_from_youtube",
    port="3306"
)

# Create a cursor to execute SQL queries
cursor = mydb.cursor()


question = st.selectbox("Select Your Question",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


if question == "1. What are the names of all the videos and their corresponding channels?":

    query1 = '''select Video_Title as videos, Channel_Name as channelname from videos'''
    cursor.execute(query1)

    # Fetch the results before committing
    table1 = cursor.fetchall()
    df_table1 = pd.DataFrame(table1, columns=["video title", "channel name"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table1)

elif question == "2. Which channels have the most number of videos, and how many videos do they have?":

    query2 = '''select channel_name as channelname,channel_videocount as no_of_videos from channels order by channel_videocount desc'''
    cursor.execute(query2)

    # Fetch the results before committing
    table2 = cursor.fetchall()
    df_table2 = pd.DataFrame(table2, columns=["Channel name", "No of Videos"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table2)


elif question == "3. What are the top 10 most viewed videos and their respective channels?":

    query3 = '''select Views as views, Channel_Name as `channel name`, Video_Title as `video title` from videos where Views is not null order by Views desc limit 10
    '''
    cursor.execute(query3)

    # Fetch the results before committing
    table3 = cursor.fetchall()
    df_table3 = pd.DataFrame(table3, columns=["views", "channel name","video title"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table3)    


elif question == "4. How many comments were made on each video, and what are their corresponding video names?":

    query4 = '''select Comment_count as no_of_comments,Video_Title as videotitle from videos where Comment_count is not null'''
    cursor.execute(query4)

    # Fetch the results before committing
    table4 = cursor.fetchall()
    df_table4 = pd.DataFrame(table4, columns=["no_of_comments", "video title"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table4)    


elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":

    query5 = '''select Video_Title as videotitle,Channel_Name as channelname,Likes as likecount from videos where Likes is not null order by Likes desc'''
    cursor.execute(query5)

    # Fetch the results before committing
    table5 = cursor.fetchall()
    df_table5 = pd.DataFrame(table5, columns=["video title", "channelname","like count"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table5)


elif question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

    query6 = '''select Likes as likecount,Video_title as videotitle from videos'''
    cursor.execute(query6)

    # Fetch the results before committing
    table6 = cursor.fetchall()
    df_table6 = pd.DataFrame(table6, columns=["like count", "video title"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table6)


elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":

    query7 = '''select channel_name as channelname,channel_viewcount as totalviews from channels'''
    cursor.execute(query7)

    # Fetch the results before committing
    table7 = cursor.fetchall()
    df_table7 = pd.DataFrame(table7, columns=["channel name", "total views"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table7)      


elif question == "8. What are the names of all the channels that have published videos in the year2022?":

    query8 = '''select Channel_Name as channelname,Published_Date as publishedin2022 from videos where extract(year from Published_Date) = 2022'''
    cursor.execute(query8)

    # Fetch the results before committing
    table8 = cursor.fetchall()
    df_table8 = pd.DataFrame(table8, columns=["channel name", "published in 2022"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table8)  



elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names":

    query9 = '''select Channel_Name as channelname,AVG(Duration) as avgduration from videos group by Channel_Name order by AVG(Duration) desc'''
    cursor.execute(query9)

    # Fetch the results before committing
    table9 = cursor.fetchall()
    df_table9 = pd.DataFrame(table9, columns=["channel name", "avg duration"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table9)    


elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":

    query10 = '''select Channel_Name as channelname,Video_Title as videotitle,Comment_count as Highest_no_of_comments from videos where Comment_count is not null order by Comment_count desc'''
    cursor.execute(query10)

    # Fetch the results before committing
    table10 = cursor.fetchall()
    df_table10 = pd.DataFrame(table10, columns=["channel name", "video title","Highest no of comments"])

    # Close the cursor
    cursor.close()

    # Commit the transaction
    mydb.commit()

    
    st.write(df_table10)




