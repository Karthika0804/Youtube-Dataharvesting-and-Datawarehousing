from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import json
from datetime import datetime
import sys
import pandas as pd
import streamlit as st
***********************************************
api_key="your api_key"
api_service_name = "youtube"
api_version = "v3"
youtube =build(api_service_name, api_version, developerKey= api_key)
**************************************************
# Function to retrieve Channel_ID
def get_channel_id(channel_name):
        response = youtube.search().list(
            part='id',
            q=channel_name,
            type='channel'
        ).execute()
        channel = response['items'][0]
        channel_id = channel['id']['channelId']
        return channel_id
# Function for one channel info
channel_id="corresponding channel_id"
def get_channel_details(channel_ids):
    all_data = []
    request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
    response = request.execute() 
    
    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    playlist_id= response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)
    
    return all_data
channel_details = get_channel_details(youtube,channel_id)
channel_details
A=pd.DataFrame(channel_details)
*****************************************************************************************
#Function to get playlistids
def get_channel_playlists(channel_id):
        response = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=100  # Adjust the maximum number of playlists 
        ).execute()
        playlists = response['items']
        channel_playlists=[]
        
        for playlist in playlists:
            data= dict(playlist_id = playlist['id'],
            playlist_title = playlist['snippet']['title'],
            )
            channel_playlists.append(data)
        return channel_playlists
playlist_Details=get_channel_playlists(channel_id)
playlist_Details
B=pd.DataFrame(playlist_Details)
********************************************************************************************************
# Function to get video_ids
playlist_id="corresponding channel_id"
def get_video_ids(youtube,channel_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults = 50
    )
    response = request.execute()
    
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = playlist_id,
                    maxResults = 50,
                    pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
        
    return (video_ids)
video_ids= get_video_ids(youtube,playlist_id)
video_ids
C=pd.DataFrame(video_ids)
***************************************************************************************
# Function to get video details
def get_video_details(youtube, video_ids):
   all_video_info = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute() 

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)
            
    return(all_video_info)
video_details=get_video_details(youtube, video_ids)
video_details
D=pd.DataFrame(video_details)
*********************************************************************************
#Function get comment details
def get_comment_data(youtube, video_ids):
    comments_data = []
    for ids in video_ids:
        try:
            video_data_request = youtube.commentThreads().list(
                part="snippet",
                videoId=ids,
                maxResults=50
            ).execute()
            video_info = video_data_request['items']
            for comment in video_info:
                comment_info = {
                    'Video_id': comment['snippet']['videoId'],
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published_At': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                }
                comments_data.append(comment_info)
        except HttpError as e:
            if e.resp.status == 403 and 'disabled comments' in str(e):
                comment_info = {
                    'Video_id': ids,
                    'Comment_Id': 'comments_disabled',
                }
                comments_data.append(comment_info)
            else:
                print(f"An error occurred while retrieving comments for video: {ids}")
                print(f"Error details: {e}")
    return comments_data
comment_details=get_comments_in_videos(youtube, video_ids)
comment_details
E=pd.DataFrame(comment_details)
**********************************************************************************************************************************************
#Final data pushing
data = {'channel_details' : channel_details[0],
            'playlist_id' : playlist_Details,
            'video_details' :video_details,
            'comment_details' : comment_details
               }
****************************************************************************************
#connection MongoDB with Python
pip install pymongo
import pymongo
client = pymongo.MongoClient("mongodb://localhost:27017")
mydb =client["your database name"]
information = mydb.data
information.insert_one(data)
**********************************************************************************************************************
#connection Mysql with MongoDB
import mysql.connector
import sqlalchemy as sq
from sqlalchemy import create_engine
import pymysql

#create a database
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="your password")
cur = myconnection.cursor()
cur.execute("create database YT_AnalysisProject")

#create a channel table to load channel information
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="your password",database="YT_AnalysisProject")
cur = myconnection.cursor()
cur.execute("CREATE TABLE channel_collection(Channel_name varchar(225),Total_Videos int,Subscribers int,Views int,Playlist_id varchar(225))")

#create new table to load playlist info
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="your password",database="YT_AnalysisProject")
cur = myconnection.cursor()
cur.execute("CREATE TABLE playlist_collection001(playlist_id varchar(225),playlist_title varchar(1000))")

#create a new table to load video information
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="your password",database="YT_AnalysisProject")
cur = myconnection.cursor()
cur.execute("CREATE TABLE Video_collection001(video_id varchar(225),channelTitle varchar(225),title varchar(225),description varchar(225),tags varchar(225),publishedAt datetime,viewCount int,likeCount int,dislikeCount varchar(225),favouriteCount varchar(225),commentCount int,duration varchar(225),definition varchar(225),caption varchar(225),pushblishDayName varchar(225),durationSecs float)")

#create a new table to load comment details
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="your password",database="YT_AnalysisProject")
cur = myconnection.cursor()
cur.execute("CREATE TABLE comment_collection001(video_id varchar(225),Comment_Id varchar(200),Comment_Text TEXT,Comment_Author varchar(100),Comment_Published_At datetime)")


#Inserting the values into the table

1.channel_collection
sql="insert into channel_collection(Channel_name,Subscribers,Views,Total_videos,playlist_id)values(%s,%s,%s,%s,%s)";
for i in range(0,len(A)):
    cur.execute(sql,tuple(A.iloc[i]))
    myconnection.commit()

2.playlist_collection
sql1="insert into playlist_collection001(playlist_id,playlist_title)values(%s,%s)";
for i in range(0,len(B)):
    cur.execute(sql1,tuple(B.iloc[i]))
    myconnection.commit()

3.video_info
sql2="insert into Video_collection001(video_id,channelTitle,title,description,tags,publishedAt,viewCount,likeCount,dislikeCount,favouriteCount,commentCount,duration,definition,caption,pushblishDayName,durationSecs)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)";
for i in range(0,len(D)):
    cur.execute(sql1,tuple(D.iloc[C]))
    myconnection.commit()

4.comment_collection
sql3="insert into comment_collection001(Video_id,Comment_Id,Comment_Text,Comment_Author,Comment_Published_At)values(%s,%s,%s,%s,%s)";
for i in range(0,len(E)):
    cur.execute(sql3,tuple(E.iloc[i]))
    myconnection.commit()
**************************************************************************************************************************
STREAM LIT DATA ANALYSIS
import streamlit as st
import pymongo
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import isodate
from dateutil import parser
st.set_page_config(layout='wide')
# Title
st.title(':red[Youtube Data Harvesting]')
channel_id=st.text_input("Enter 11 digit channel_id")
option = st.radio(
    'Select any one to perform',
    ('Home','Getdata','Migratedata','Querydata')) 
if option == "Getdata":
        api_service_name = 'youtube'
        api_version = 'v3'
        api_key = 'AIzaSyAsXiok1bOJ7ffrOMRNVw3NEalhummXM_4'
        youtube = build(api_service_name,api_version,developerKey =api_key)
        def get_channel_details(channel_id):
            all_data = []
            request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id)
            response = request.execute() 
            if 'items' not in response:
                st.write(f"Invalid channel id: {channel_id}")
                st.error("Enter the correct 11-digit **channel_id**")
                return None
            if 'items' in response:
                for i in range(len(response['items'])):
                    data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    playlist_id= response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
            all_data.append(data)
            return all_data
        
        def get_channel_playlists(channel_id):
            response = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=100  # Adjust the maximum number of playlists 
        ).execute()
            playlists = response['items']
            channel_playlists=[]
        
            for playlist in playlists:
               data= dict(playlist_id = playlist['id'],
               playlist_title = playlist['snippet']['title'],
               )
               channel_playlists.append(data)
            return channel_playlists
        
        def get_video_ids(youtube, playlist_id):
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
            response=request.execute()
            video_ids = []
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
            next_page_token = response.get('nextPageToken')
            more_pages = True
    
            while more_pages:
                 if next_page_token is None:
                     more_pages = False
                 else:
                    request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
                    response = request.execute()
    
                    for i in range(len(response['items'])):
                        video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
                    next_page_token = response.get('nextPageToken')
        
            return video_ids
        
        def get_video_details(youtube,video_ids):
                all_video_info = []
                for i in range(0, len(video_ids), 50):
                    request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50])
                  )
                    response = request.execute() 

                for video in response['items']:
                    stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount','dislikeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
                    video_info = {}
                    video_info['video_id'] = video['id']

                for k in stats_to_keep.keys():
                    for v in stats_to_keep[k]:
                       try:
                        video_info[v] = video[k][v]
                       except:
                        video_info[v] = None

                all_video_info.append(video_info)
            
                return all_video_info
        
        def get_comment_data(youtube, video_ids):
            comments_data = []
            for ids in video_ids:
                try: 
                  video_data_request = youtube.commentThreads().list(
                  part="snippet",
                  videoId=ids,
                  maxResults=50
                  ).execute()
                  video_info = video_data_request['items']
                  for comment in video_info:
                      comment_info = {
                           'Video_id': comment['snippet']['videoId'],
                            'Comment_Id': comment['snippet']['topLevelComment']['id'],
                            'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'Comment_Published_At': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                        }
                      comments_data.append(comment_info)
                except HttpError as e:
                    if e.resp.status == 403 and 'disabled comments' in str(e):
                        comment_info = {
                            'Video_id': ids,
                            'Comment_Id': 'comments_disabled',
                             }
                        comments_data.append(comment_info)
                    else:
                      print(f"An error occurred while retrieving comments for video: {ids}")
                      print(f"Error details: {e}")
            return comments_data
        
       
        channel_details = get_channel_details(channel_id)
        A=pd.DataFrame(channel_details)
        st.dataframe(A)
                 
        playlist_Details=get_channel_playlists(channel_id)
        B= pd.DataFrame(playlist_Details)
        st.dataframe(B)
               
        playlist_id =A['playlist_id'][0]
        video_ids=get_video_ids(youtube,playlist_id)
        C=pd.DataFrame(video_ids)
        st.dataframe(C)
                 
        video_details=get_video_details(youtube,video_ids)
        D=pd.DataFrame(video_details)
        D['publishedAt'] = D['publishedAt'].apply(lambda x: parser.parse(x)) 
        D['pushblishDayName'] =D['publishedAt'].apply(lambda x: x.strftime("%A")) 
        D['durationSecs'] = D['duration'].apply(lambda x: isodate.parse_duration(x))
        D['durationSecs'] = D['durationSecs'].astype(str)
        video_details=D.to_dict("records")
        st.dataframe(D)

        comment_details=get_comment_data(youtube,video_ids)
        E=pd.DataFrame(comment_details)
        E['Comment_Published_At'] = E['Comment_Published_At'].apply(lambda x: parser.parse(x)) 
        comment_details=E.to_dict("records")
        st.dataframe(E)
        
# create a client instance of MongoDB
        client = pymongo.MongoClient('mongodb://localhost:27017/')

        # create a database or use existing one
        mydb = client['Youtube_process']
        # create a collection
        collection = mydb['Youtube_data']
        # define the data to insert
        data = {'Channel_name' :A["Channel_name"][0],
                          'channel_details' : channel_details,
                          'playlist_id' : playlist_Details,
                           'video_details' :video_details,
                            'comment_details' : comment_details
                       }

        # insert or update data in the collection
        upload = collection.replace_one({'_id': channel_id},data, upsert=True)

        # print the result of the insertion operation
        st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")

        # Close the connection
        client.close()
        
if option=="Migratedata":
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client['Youtube_process']
    collection = mydb['Youtube_data']
    document_names = []
    for document in collection.find():
        document_names.append(document['Channel_name'])
    document_name = st.selectbox('**Select Channel_name**', options = document_names, key='document_names')
    st.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
    Migrate = st.button('**Migrate to MySQL**')
    
     # Define Session state to Migrate to MySQL button
    if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
    if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True
        result = collection.find_one({"Channel_name": document_name})
        client.close()




   







